/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.loadbalance.extensions.scheduler;

import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Failure;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Skip;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.CoolDown;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.HitCount;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBrokers;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBundles;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoLoadData;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.OutDatedData;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Overloaded;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Underloaded;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Unknown;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannel;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.extensions.data.TopBundlesLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision;
import org.apache.pulsar.broker.loadbalance.extensions.policies.AntiAffinityGroupPolicyHelper;
import org.apache.pulsar.broker.loadbalance.extensions.policies.IsolationPoliciesHelper;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.broker.loadbalance.impl.SimpleResourceAllocationPolicies;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load shedding strategy that unloads bundles from the highest loaded brokers.
 * This strategy is only configurable in the broker load balancer extenstions introduced by
 * PIP-192[https://github.com/apache/pulsar/issues/16691].
 *
 * This load shedding strategy has the following goals:
 * 1. Distribute bundle load across brokers in order to make the standard deviation of the avg resource usage,
 * std(exponential-moving-avg(max(cpu, memory, network, throughput)) for each broker) below the target,
 * configurable by loadBalancerBrokerLoadTargetStd.
 * 2. Use the transfer protocol to transfer bundle load from the highest loaded to the lowest loaded brokers,
 * if configured by loadBalancerTransferEnabled=true.
 * 3. Avoid repeated bundle unloading by recomputing historical broker resource usage after unloading and also
 * skipping the bundles that are recently unloaded.
 * 4. Prioritize unloading bundles to underloaded brokers when their message throughput is zero(new brokers).
 * 5. Do not use outdated broker load data (configurable by loadBalancerBrokerLoadDataTTLInSeconds).
 * 6. Give enough time for each broker to recompute its load after unloading
 * (configurable by loadBalanceUnloadDelayInSeconds)
 * 7. Do not transfer bundles with namespace isolation policies or anti-affinity group policies.
 * 8. Limit the max number of brokers to transfer bundle load for each cycle,
 * (loadBalancerMaxNumberOfBrokerTransfersPerCycle).
 * 9. Print more logs with a debug option(loadBalancerDebugModeEnabled=true).
 */
@NoArgsConstructor
public class TransferShedder implements NamespaceUnloadStrategy {
    private static final Logger log = LoggerFactory.getLogger(TransferShedder.class);
    private static final double KB = 1024;
    private static final String CANNOT_CONTINUE_UNLOAD_MSG = "Can't continue the unload cycle.";
    private static final String CANNOT_UNLOAD_BROKER_MSG = "Can't unload broker:%s.";
    private static final String CANNOT_UNLOAD_BUNDLE_MSG = "Can't unload bundle:%s.";
    private final LoadStats stats = new LoadStats();
    private PulsarService pulsar;
    private SimpleResourceAllocationPolicies allocationPolicies;
    private IsolationPoliciesHelper isolationPoliciesHelper;
    private AntiAffinityGroupPolicyHelper antiAffinityGroupPolicyHelper;
    private Set<UnloadDecision> decisionCache;
    private UnloadCounter counter;
    private ServiceUnitStateChannel channel;
    private int unloadConditionHitCount = 0;

    @VisibleForTesting
    public TransferShedder(UnloadCounter counter){
        this.pulsar = null;
        this.decisionCache = new HashSet<>();
        this.allocationPolicies = null;
        this.counter = counter;
        this.isolationPoliciesHelper = null;
        this.antiAffinityGroupPolicyHelper = null;
    }

    public TransferShedder(PulsarService pulsar,
                           UnloadCounter counter,
                           AntiAffinityGroupPolicyHelper antiAffinityGroupPolicyHelper){
        this.pulsar = pulsar;
        this.decisionCache = new HashSet<>();
        this.allocationPolicies = new SimpleResourceAllocationPolicies(pulsar);
        this.counter = counter;
        this.isolationPoliciesHelper = new IsolationPoliciesHelper(allocationPolicies);
        this.channel = ServiceUnitStateChannelImpl.get(pulsar);
        this.antiAffinityGroupPolicyHelper = antiAffinityGroupPolicyHelper;
    }


    @Getter
    @Accessors(fluent = true)
    static class LoadStats {
        private double sum;
        private double sqSum;
        private int totalBrokers;
        private double avg;
        private double std;
        private LoadDataStore<BrokerLoadData> loadDataStore;
        private  List<Map.Entry<String, BrokerLoadData>> brokersSortedByLoad;
        int maxBrokerIndex;
        int minBrokerIndex;
        int numberOfBrokerSheddingPerCycle;
        int maxNumberOfBrokerSheddingPerCycle;
        LoadStats() {
            brokersSortedByLoad = new ArrayList<>();
        }

        private void update(double sum, double sqSum, int totalBrokers) {
            this.sum = sum;
            this.sqSum = sqSum;
            this.totalBrokers = totalBrokers;

            if (totalBrokers == 0) {
                this.avg = 0;
                this.std = 0;

            } else {
                this.avg = sum / totalBrokers;
                this.std = Math.sqrt(sqSum / totalBrokers - avg * avg);
            }
        }

        void offload(double max, double min, double offload) {
            sqSum -= max * max + min * min;
            double maxd = Math.max(0, max - offload);
            double mind = min + offload;
            sqSum += maxd * maxd + mind * mind;
            std = Math.sqrt(sqSum / totalBrokers - avg * avg);
            numberOfBrokerSheddingPerCycle++;
            minBrokerIndex++;
        }

        void clear() {
            sum = 0.0;
            sqSum = 0.0;
            totalBrokers = 0;
            avg = 0.0;
            std = 0.0;
            maxBrokerIndex = 0;
            minBrokerIndex = 0;
            numberOfBrokerSheddingPerCycle = 0;
            maxNumberOfBrokerSheddingPerCycle = 0;
            brokersSortedByLoad.clear();
            loadDataStore = null;
        }

        Optional<UnloadDecision.Reason> update(final LoadDataStore<BrokerLoadData> loadStore,
                                               final Map<String, BrokerLookupData> availableBrokers,
                                               Map<String, Long> recentlyUnloadedBrokers,
                                               final ServiceConfiguration conf) {

            maxNumberOfBrokerSheddingPerCycle = conf.getLoadBalancerMaxNumberOfBrokerSheddingPerCycle();
            var debug = ExtensibleLoadManagerImpl.debug(conf, log);
            UnloadDecision.Reason decisionReason = null;
            double sum = 0.0;
            double sqSum = 0.0;
            int totalBrokers = 0;
            long now = System.currentTimeMillis();
            var missingLoadDataBrokers = new HashSet<>(availableBrokers.keySet());
            for (Map.Entry<String, BrokerLoadData> entry : loadStore.entrySet()) {
                BrokerLoadData localBrokerData = entry.getValue();
                String broker = entry.getKey();
                missingLoadDataBrokers.remove(broker);
                // We don't want to use the outdated load data.
                if (now - localBrokerData.getUpdatedAt()
                        > conf.getLoadBalancerBrokerLoadDataTTLInSeconds() * 1000) {
                    log.warn(
                            "Ignoring broker:{} load update because the load data timestamp:{} is too old.",
                            broker, localBrokerData.getUpdatedAt());
                    decisionReason = OutDatedData;
                    continue;
                }

                // Also, we should give enough time for each broker to recompute its load after transfers.
                if (recentlyUnloadedBrokers.containsKey(broker)) {
                    var elapsed = localBrokerData.getUpdatedAt() - recentlyUnloadedBrokers.get(broker);
                    if (elapsed < conf.getLoadBalanceSheddingDelayInSeconds() * 1000) {
                        if (debug) {
                            log.warn(
                                    "Broker:{} load data is too early since "
                                            + "the last transfer. elapsed {} secs < threshold {} secs",
                                    broker,
                                    TimeUnit.MILLISECONDS.toSeconds(elapsed),
                                    conf.getLoadBalanceSheddingDelayInSeconds());
                        }
                        update(0.0, 0.0, 0);
                        return Optional.of(CoolDown);
                    } else {
                        recentlyUnloadedBrokers.remove(broker);
                    }
                }

                double load = localBrokerData.getWeightedMaxEMA();

                sum += load;
                sqSum += load * load;
                totalBrokers++;
            }

            if (totalBrokers == 0) {
                if (decisionReason == null) {
                    decisionReason = NoBrokers;
                }
                update(0.0, 0.0, 0);
                if (debug) {
                    log.info("There is no broker load data.");
                }
                return Optional.of(decisionReason);
            }

            if (!missingLoadDataBrokers.isEmpty()) {
                decisionReason = NoLoadData;
                update(0.0, 0.0, 0);
                if (debug) {
                    log.info("There is missing load data from brokers:{}", missingLoadDataBrokers);
                }
                return Optional.of(decisionReason);
            }

            update(sum, sqSum, totalBrokers);
            return Optional.empty();
        }

        void setLoadDataStore(LoadDataStore<BrokerLoadData> loadDataStore) {
            this.loadDataStore = loadDataStore;
            brokersSortedByLoad.addAll(loadDataStore.entrySet());
            Collections.sort(brokersSortedByLoad, (a, b) -> Double.compare(
                    a.getValue().getWeightedMaxEMA(),
                    b.getValue().getWeightedMaxEMA()));
            maxBrokerIndex = brokersSortedByLoad.size() - 1;
            minBrokerIndex = 0;
        }

        String peekMinBroker() {
            return brokersSortedByLoad.get(minBrokerIndex).getKey();
        }

        String pollMaxBroker() {
            return brokersSortedByLoad.get(maxBrokerIndex--).getKey();
        }

        @Override
        public String toString() {
            return String.format(
                    "sum:%.2f, sqSum:%.2f, avg:%.2f, std:%.2f, totalBrokers:%d, brokersSortedByLoad:%s",
                    sum, sqSum, avg, std, totalBrokers,
                    brokersSortedByLoad.stream().map(v->v.getKey()).collect(Collectors.toList()));
        }


        boolean hasTransferableBrokers() {
            return numberOfBrokerSheddingPerCycle < maxNumberOfBrokerSheddingPerCycle
                    && minBrokerIndex < maxBrokerIndex;
        }
    }


    @Override
    public Set<UnloadDecision> findBundlesForUnloading(LoadManagerContext context,
                                                  Map<String, Long> recentlyUnloadedBundles,
                                                  Map<String, Long> recentlyUnloadedBrokers) {
        final var conf = context.brokerConfiguration();
        decisionCache.clear();
        stats.clear();
        Map<String, BrokerLookupData> availableBrokers;
        try {
            availableBrokers = context.brokerRegistry().getAvailableBrokerLookupDataAsync()
                    .get(context.brokerConfiguration().getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            counter.update(Failure, Unknown);
            log.warn("Failed to fetch available brokers. Stop unloading.", e);
            return decisionCache;
        }

        try {
            final var loadStore = context.brokerLoadDataStore();
            stats.setLoadDataStore(loadStore);
            boolean debugMode = ExtensibleLoadManagerImpl.debug(conf, log);

            var skipReason = stats.update(
                    context.brokerLoadDataStore(), availableBrokers, recentlyUnloadedBrokers, conf);
            if (skipReason.isPresent()) {
                if (debugMode) {
                    log.warn(CANNOT_CONTINUE_UNLOAD_MSG
                                    + " Skipped the load stat update. Reason:{}.",
                            skipReason.get());
                }
                counter.update(Skip, skipReason.get());
                return decisionCache;
            }
            counter.updateLoadData(stats.avg, stats.std);



            if (debugMode) {
                log.info("brokers' load stats:{}", stats);
            }

            // skip metrics
            int numOfBrokersWithEmptyLoadData = 0;
            int numOfBrokersWithFewBundles = 0;

            final double targetStd = conf.getLoadBalancerBrokerLoadTargetStd();
            boolean transfer = conf.isLoadBalancerTransferEnabled();
            if (stats.std() > targetStd || isUnderLoaded(context, stats.peekMinBroker(), stats.avg)) {
                unloadConditionHitCount++;
            } else {
                unloadConditionHitCount = 0;
            }

            if (unloadConditionHitCount <= conf.getLoadBalancerSheddingConditionHitCountThreshold()) {
                if (debugMode) {
                    log.info(CANNOT_CONTINUE_UNLOAD_MSG
                                    + " Shedding condition hit count:{} is less than the threshold:{}.",
                            unloadConditionHitCount, conf.getLoadBalancerSheddingConditionHitCountThreshold());
                }
                counter.update(Skip, HitCount);
                return decisionCache;
            }

            while (true) {
                if (!stats.hasTransferableBrokers()) {
                    if (debugMode) {
                        log.info(CANNOT_CONTINUE_UNLOAD_MSG
                                + " Exhausted target transfer brokers.");
                    }
                    break;
                }
                UnloadDecision.Reason reason;
                if (stats.std() <= targetStd) {
                    if (!isUnderLoaded(context, stats.peekMinBroker(), stats.avg)) {
                        if (debugMode) {
                            log.info(CANNOT_CONTINUE_UNLOAD_MSG
                                            + " std:{} <= targetStd:{} and minBroker:{} has msg throughput.",
                                    stats.std(), targetStd, stats.peekMinBroker());
                        }
                        break;
                    } else {
                        reason = Underloaded;
                        if (debugMode) {
                            log.info(String.format("broker:%s is underloaded:%s although "
                                            + "load std:%.2f <= targetStd:%.2f. "
                                            + "Continuing unload for this underloaded broker.",
                                    stats.peekMinBroker(),
                                    context.brokerLoadDataStore().get(stats.peekMinBroker()).get(),
                                    stats.std(), targetStd));
                        }
                    }
                } else {
                    reason = Overloaded;
                }

                String maxBroker = stats.pollMaxBroker();
                String minBroker = stats.peekMinBroker();
                Optional<BrokerLoadData> maxBrokerLoadData = context.brokerLoadDataStore().get(maxBroker);
                Optional<BrokerLoadData> minBrokerLoadData = context.brokerLoadDataStore().get(minBroker);
                if (maxBrokerLoadData.isEmpty()) {
                    log.error(String.format(CANNOT_UNLOAD_BROKER_MSG
                            + " MaxBrokerLoadData is empty.", maxBroker));
                    numOfBrokersWithEmptyLoadData++;
                    continue;
                }
                if (minBrokerLoadData.isEmpty()) {
                    log.error("Can't transfer load to broker:{}. MinBrokerLoadData is empty.", minBroker);
                    numOfBrokersWithEmptyLoadData++;
                    continue;
                }
                double max = maxBrokerLoadData.get().getWeightedMaxEMA();
                double min = minBrokerLoadData.get().getWeightedMaxEMA();
                double offload = (max - min) / 2 / max;
                BrokerLoadData brokerLoadData = maxBrokerLoadData.get();
                double brokerThroughput = brokerLoadData.getMsgThroughputIn() + brokerLoadData.getMsgThroughputOut();
                double offloadThroughput = brokerThroughput * offload;

                if (debugMode) {
                    log.info(String.format(
                            "Attempting to shed load from broker:%s%s, which has the max resource "
                                    + "usage:%.2f%%, targetStd:%.2f,"
                                    + " -- Trying to offload %.2f%%, %.2f KByte/s of traffic, "
                                    + "left throughput %.2f KByte/s",
                            maxBroker, transfer ? " to broker:" + minBroker : "",
                            max * 100,
                            targetStd,
                            offload * 100,
                            offloadThroughput / KB,
                            (brokerThroughput - offloadThroughput) / KB
                    ));
                }

                double trafficMarkedToOffload = 0;

                Optional<TopBundlesLoadData> bundlesLoadData = context.topBundleLoadDataStore().get(maxBroker);
                if (bundlesLoadData.isEmpty() || bundlesLoadData.get().getTopBundlesLoadData().isEmpty()) {
                    log.error(String.format(CANNOT_UNLOAD_BROKER_MSG
                            + " TopBundlesLoadData is empty.", maxBroker));
                    numOfBrokersWithEmptyLoadData++;
                    continue;
                }

                var topBundlesLoadData = bundlesLoadData.get().getTopBundlesLoadData();
                if (topBundlesLoadData.size() == 1) {
                    numOfBrokersWithFewBundles++;
                    log.warn(String.format(CANNOT_UNLOAD_BROKER_MSG
                                    + " Sole namespace bundle:%s is overloading the broker. ",
                            maxBroker, topBundlesLoadData.iterator().next()));
                    continue;
                }

                if (topBundlesLoadData.isEmpty()) {
                    numOfBrokersWithFewBundles++;
                    log.warn(String.format(CANNOT_UNLOAD_BROKER_MSG
                            + " Broker overloaded despite having no bundles", maxBroker));
                    continue;
                }

                int remainingTopBundles = topBundlesLoadData.size();
                for (var e : topBundlesLoadData) {
                    String bundle = e.bundleName();
                    if (!channel.isOwner(bundle, maxBroker)) {
                        if (debugMode) {
                            log.warn(String.format(CANNOT_UNLOAD_BUNDLE_MSG
                                    + " MaxBroker:%s is not the owner.", bundle, maxBroker));
                        }
                        continue;
                    }
                    if (recentlyUnloadedBundles.containsKey(bundle)) {
                        if (debugMode) {
                            log.info(String.format(CANNOT_UNLOAD_BUNDLE_MSG
                                            + " Bundle has been recently unloaded at ts:%d.",
                                    bundle, recentlyUnloadedBundles.get(bundle)));
                        }
                        continue;
                    }
                    if (!isTransferable(context, availableBrokers, bundle, maxBroker, Optional.of(minBroker))) {
                        if (debugMode) {
                            log.info(String.format(CANNOT_UNLOAD_BUNDLE_MSG
                                    + " This unload can't meet "
                                    + "affinity(isolation) or anti-affinity group policies.", bundle));
                        }
                        continue;
                    }
                    if (remainingTopBundles <= 1) {
                        if (debugMode) {
                            log.info(String.format(CANNOT_UNLOAD_BUNDLE_MSG
                                            + " The remaining bundles in TopBundlesLoadData from the maxBroker:%s is"
                                            + " less than or equal to 1.",
                                    bundle, maxBroker));
                        }
                        break;
                    }

                    var bundleData = e.stats();
                    double throughput = bundleData.msgThroughputIn + bundleData.msgThroughputOut;

                    if (throughput + trafficMarkedToOffload > offloadThroughput) {
                        if (debugMode) {
                            log.info(String.format(CANNOT_UNLOAD_BUNDLE_MSG
                                            + " The traffic to unload:%.2f KByte/s is "
                                            + "greater than threshold:%.2f KByte/s.",
                                    bundle, (throughput + trafficMarkedToOffload) / KB, offloadThroughput / KB));
                        }
                        break;
                    }

                    Unload unload;
                    if (transfer) {
                        unload = new Unload(maxBroker, bundle, Optional.of(minBroker));
                    } else {
                        unload = new Unload(maxBroker, bundle);
                    }
                    var decision = new UnloadDecision();
                    decision.setUnload(unload);
                    decision.succeed(reason);
                    decisionCache.add(decision);
                    trafficMarkedToOffload += throughput;
                    remainingTopBundles--;

                    if (debugMode) {
                        log.info(String.format("Decided to unload bundle:%s, throughput:%.2f KByte/s."
                                        + " The traffic marked to unload:%.2f KByte/s."
                                        + " Threshold:%.2f KByte/s.",
                                bundle, throughput / KB, trafficMarkedToOffload / KB, offloadThroughput / KB));
                    }
                }
                if (trafficMarkedToOffload > 0) {
                    stats.offload(max, min, offload);
                    if (debugMode) {
                        log.info(
                                String.format("brokers' load stats:%s, after offload{max:%.2f, min:%.2f, offload:%.2f}",
                                        stats, max, min, offload));
                    }
                } else {
                    numOfBrokersWithFewBundles++;
                    log.warn(String.format(CANNOT_UNLOAD_BROKER_MSG
                            + " There is no bundle that can be unloaded in top bundles load data. "
                            + "Consider splitting bundles owned by the broker "
                            + "to make each bundle serve less traffic "
                            + "or increasing loadBalancerMaxNumberOfBundlesInBundleLoadReport"
                            + " to report more bundles in the top bundles load data.", maxBroker));
                }

            } // while end

            if (debugMode) {
                log.info("decisionCache:{}", decisionCache);
            }

            if (decisionCache.isEmpty()) {
                UnloadDecision.Reason reason;
                if (numOfBrokersWithEmptyLoadData > 0) {
                    reason = NoLoadData;
                } else if (numOfBrokersWithFewBundles > 0) {
                    reason = NoBundles;
                } else {
                    reason = HitCount;
                }
                counter.update(Skip, reason);
            } else {
                unloadConditionHitCount = 0;
            }

        } catch (Throwable e) {
            log.error("Failed to process unloading. ", e);
            this.counter.update(Failure, Unknown);
        }
        return decisionCache;
    }


    private boolean isUnderLoaded(LoadManagerContext context, String broker, double avgLoad) {
        var brokerLoadDataOptional = context.brokerLoadDataStore().get(broker);
        if (brokerLoadDataOptional.isEmpty()) {
            return false;
        }
        var brokerLoadData = brokerLoadDataOptional.get();
        if (brokerLoadData.getMsgThroughputEMA() < 1) {
            return true;
        }

        return brokerLoadData.getWeightedMaxEMA()
                < avgLoad * Math.min(0.5, Math.max(0.0,
                context.brokerConfiguration().getLoadBalancerBrokerLoadTargetStd()));
    }


    private boolean isTransferable(LoadManagerContext context,
                                   Map<String, BrokerLookupData> availableBrokers,
                                   String bundle,
                                   String srcBroker,
                                   Optional<String> dstBroker) {
        if (pulsar == null || allocationPolicies == null) {
            return true;
        }

        String namespace = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
        final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundle);
        NamespaceBundle namespaceBundle =
                pulsar.getNamespaceService().getNamespaceBundleFactory().getBundle(namespace, bundleRange);

        if (!canTransferWithIsolationPoliciesToBroker(
                context, availableBrokers, namespaceBundle, srcBroker, dstBroker)) {
            return false;
        }

        if (!antiAffinityGroupPolicyHelper.canUnload(availableBrokers, bundle, srcBroker, dstBroker)) {
            return false;
        }
        return true;
    }

    /**
     * Check the gave bundle and broker can be transfer or unload with isolation policies applied.
     *
     * @param context The load manager context.
     * @param availableBrokers The available brokers.
     * @param namespaceBundle The bundle try to unload or transfer.
     * @param currentBroker The current broker.
     * @param targetBroker The broker will be transfer to.
     * @return Can be transfer/unload or not.
     */
    private boolean canTransferWithIsolationPoliciesToBroker(LoadManagerContext context,
                                                             Map<String, BrokerLookupData> availableBrokers,
                                                             NamespaceBundle namespaceBundle,
                                                             String currentBroker,
                                                             Optional<String> targetBroker) {
        if (isolationPoliciesHelper == null
                || !allocationPolicies.areIsolationPoliciesPresent(namespaceBundle.getNamespaceObject())) {
            return true;
        }

        // bundle has isolation policies.
        if (!context.brokerConfiguration().isLoadBalancerSheddingBundlesWithPoliciesEnabled()) {
            return false;
        }

        boolean transfer = context.brokerConfiguration().isLoadBalancerTransferEnabled();
        Set<String> candidates = isolationPoliciesHelper.applyIsolationPolicies(availableBrokers, namespaceBundle);

        // Remove the current bundle owner broker.
        candidates.remove(currentBroker);

        // Unload: Check if there are any more candidates available for selection.
        if (targetBroker.isEmpty() || !transfer) {
            return !candidates.isEmpty();
        }
        // Transfer: Check if this broker is among the candidates.
        return candidates.contains(targetBroker.get());
    }
}
