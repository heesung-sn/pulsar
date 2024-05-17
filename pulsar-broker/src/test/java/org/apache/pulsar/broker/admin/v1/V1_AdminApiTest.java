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
package org.apache.pulsar.broker.admin.v1;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.hash.Hashing;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.broker.testcontext.SpyConfig;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.client.admin.internal.LookupImpl;
import org.apache.pulsar.client.admin.internal.TenantsImpl;
import org.apache.pulsar.client.admin.internal.TopicsImpl;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.BacklogQuota.RetentionPolicy;
import org.apache.pulsar.common.policies.data.BrokerAssignment;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.NamespaceOwnershipStatus;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PoliciesUtil;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.compaction.PulsarCompactionServiceFactory;
import org.apache.pulsar.metadata.cache.impl.MetadataCacheImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-admin")
public class V1_AdminApiTest extends MockedPulsarServiceBaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(V1_AdminApiTest.class);

    private MockedPulsarService mockPulsarSetup;

    private PulsarService otherPulsar;

    private PulsarAdmin adminTls;
    private PulsarAdmin otheradmin;


    @BeforeClass
    @Override
    public void setup() throws Exception {
        conf.setTopicLevelPoliciesEnabled(false);
        conf.setSystemTopicEnabled(false);
        conf.setLoadBalancerEnabled(true);
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        conf.setNumExecutorThreadPoolSize(5);

        super.internalSetup();

        adminTls = spy(PulsarAdmin.builder().tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .serviceHttpUrl(brokerUrlTls.toString()).build());

        // create otherbroker to test redirect on calls that need
        // namespace ownership
        mockPulsarSetup = new MockedPulsarService(this.conf);
        mockPulsarSetup.setup();
        otherPulsar = mockPulsarSetup.getPulsar();
        otheradmin = mockPulsarSetup.getAdmin();

        // Setup namespaces
        admin.clusters().createCluster("use", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("use"));
        admin.tenants().createTenant("prop-xyz", tenantInfo);
        admin.namespaces().createNamespace("prop-xyz/use/ns1");
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        pulsar.getConfiguration().setBrokerShutdownTimeoutMs(0);
        adminTls.close();
        otheradmin.close();
        super.internalCleanup();
        mockPulsarSetup.cleanup();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        pulsarTestContextBuilder.spyConfigCustomizer(
                // verify(compactor) is used in this test class
                builder -> builder.compactor(SpyConfig.SpyType.SPY_ALSO_INVOCATIONS));
    }

    @AfterMethod(alwaysRun = true)
    public void reset() throws Exception {
        pulsar.getConfiguration().setForceDeleteNamespaceAllowed(true);
        for (String tenant : admin.tenants().getTenants()) {
            if (tenant.equals("pulsar")) {
                continue;
            }
            for (String namespace : admin.namespaces().getNamespaces(tenant)) {
                deleteNamespaceWithRetry(namespace, true, admin, pulsar,
                        mockPulsarSetup.getPulsar());
            }
        }
        pulsar.getConfiguration().setForceDeleteNamespaceAllowed(false);

        resetConfig();

        if (!admin.clusters().getClusters().contains("use")) {
            admin.clusters().createCluster("use",
                    ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        }

        if (!admin.tenants().getTenants().contains("prop-xyz")) {
            TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("use"));
            admin.tenants().createTenant("prop-xyz", tenantInfo);
        }
        admin.namespaces().createNamespace("prop-xyz/use/ns1");
    }

    @DataProvider(name = "numBundles")
    public static Object[][] numBundles() {
        return new Object[][] { { 1 }, { 4 } };
    }

    @DataProvider(name = "bundling")
    public static Object[][] bundling() {
        return new Object[][] { { 0 }, { 4 } };
    }

    @DataProvider(name = "topicName")
    public Object[][] topicNamesProvider() {
        return new Object[][] { { "topic_+&*%{}() \\/$@#^%" }, { "simple-topicName" } };
    }

    @DataProvider(name = "topicType")
    public Object[][] topicTypeProvider() {
        return new Object[][] { { TopicDomain.persistent.value() }, { TopicDomain.non_persistent.value() } };
    }


    @Test
    public void testNamespaceSplitBundle() throws Exception {
        // Force to create a topic
        final String namespace = "prop-xyz/use/ns1";
        final String topicName = "persistent://" + namespace + "/ds2";
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        producer.send("message".getBytes());
        publishMessagesOnPersistentTopic(topicName, 0);
        assertEquals(admin.topics().getList(namespace), List.of(topicName));

        try {
            admin.namespaces().splitNamespaceBundle(namespace, "0x00000000_0xffffffff", true, null);
        } catch (Exception e) {
            fail("split bundle shouldn't have thrown exception");
        }

        // bundle-factory cache must have updated split bundles
        NamespaceBundles bundles = pulsar.getNamespaceService().getNamespaceBundleFactory().getBundles(NamespaceName.get(namespace));
        String[] splitRange = { namespace + "/0x00000000_0x7fffffff", namespace + "/0x7fffffff_0xffffffff" };
        for (int i = 0; i < bundles.getBundles().size(); i++) {
            assertEquals(bundles.getBundles().get(i).toString(), splitRange[i]);
        }

        producer.close();
    }

    @Test
    public void testNamespaceSplitBundleConcurrent() throws Exception {
        // Force to create a topic
        final String namespace = "prop-xyz/use/ns1";
        final String topicName = "persistent://" + namespace + "/ds2";
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        producer.send("message".getBytes());
        publishMessagesOnPersistentTopic(topicName, 0);
        assertEquals(admin.topics().getList(namespace), List.of(topicName));

        try {
            admin.namespaces().splitNamespaceBundle(namespace, "0x00000000_0xffffffff", false, null);
        } catch (Exception e) {
            fail("split bundle shouldn't have thrown exception");
        }

        // bundle-factory cache must have updated split bundles
        NamespaceBundles bundles = pulsar.getNamespaceService().getNamespaceBundleFactory().getBundles(NamespaceName.get(namespace));
        String[] splitRange = {namespace + "/0x00000000_0x7fffffff", namespace + "/0x7fffffff_0xffffffff"};
        for (int i = 0; i < bundles.getBundles().size(); i++) {
            assertEquals(bundles.getBundles().get(i).toString(), splitRange[i]);
        }

        @Cleanup("shutdownNow")
        ExecutorService executorService = Executors.newCachedThreadPool();


        try {
            executorService.invokeAll(
                Arrays.asList(
                    () ->
                    {
                        log.info("split 2 bundles at the same time. spilt: 0x00000000_0x7fffffff ");
                        admin.namespaces().splitNamespaceBundle(namespace, "0x00000000_0x7fffffff", false, null);
                        return null;
                    },
                    () ->
                    {
                        log.info("split 2 bundles at the same time. spilt: 0x7fffffff_0xffffffff ");
                        admin.namespaces().splitNamespaceBundle(namespace, "0x7fffffff_0xffffffff", false, null);
                        return null;
                    }
                )
            );
        } catch (Exception e) {
            fail("split bundle shouldn't have thrown exception");
        }

        String[] splitRange4 = {
            namespace + "/0x00000000_0x3fffffff",
            namespace + "/0x3fffffff_0x7fffffff",
            namespace + "/0x7fffffff_0xbfffffff",
            namespace + "/0xbfffffff_0xffffffff"};
        bundles = pulsar.getNamespaceService().getNamespaceBundleFactory().getBundles(NamespaceName.get(namespace));
        assertEquals(bundles.getBundles().size(), 4);
        for (int i = 0; i < bundles.getBundles().size(); i++) {
            assertEquals(bundles.getBundles().get(i).toString(), splitRange4[i]);
        }

        try {
            executorService.invokeAll(
                Arrays.asList(
                    () ->
                    {
                        log.info("split 4 bundles at the same time. spilt: 0x00000000_0x3fffffff ");
                        admin.namespaces().splitNamespaceBundle(namespace, "0x00000000_0x3fffffff", false, null);
                        return null;
                    },
                    () ->
                    {
                        log.info("split 4 bundles at the same time. spilt: 0x3fffffff_0x7fffffff ");
                        admin.namespaces().splitNamespaceBundle(namespace, "0x3fffffff_0x7fffffff", false, null);
                        return null;
                    },
                    () ->
                    {
                        log.info("split 4 bundles at the same time. spilt: 0x7fffffff_0xbfffffff ");
                        admin.namespaces().splitNamespaceBundle(namespace, "0x7fffffff_0xbfffffff", false, null);
                        return null;
                    },
                    () ->
                    {
                        log.info("split 4 bundles at the same time. spilt: 0xbfffffff_0xffffffff ");
                        admin.namespaces().splitNamespaceBundle(namespace, "0xbfffffff_0xffffffff", false, null);
                        return null;
                    }
                )
            );
        } catch (Exception e) {
            fail("split bundle shouldn't have thrown exception");
        }

        String[] splitRange8 = {
            namespace + "/0x00000000_0x1fffffff",
            namespace + "/0x1fffffff_0x3fffffff",
            namespace + "/0x3fffffff_0x5fffffff",
            namespace + "/0x5fffffff_0x7fffffff",
            namespace + "/0x7fffffff_0x9fffffff",
            namespace + "/0x9fffffff_0xbfffffff",
            namespace + "/0xbfffffff_0xdfffffff",
            namespace + "/0xdfffffff_0xffffffff"};
        bundles = pulsar.getNamespaceService().getNamespaceBundleFactory().getBundles(NamespaceName.get(namespace));
        assertEquals(bundles.getBundles().size(), 8);
        for (int i = 0; i < bundles.getBundles().size(); i++) {
            assertEquals(bundles.getBundles().get(i).toString(), splitRange8[i]);
        }

        producer.close();
    }



    private void publishMessagesOnPersistentTopic(String topicName, int messages) throws Exception {
        publishMessagesOnPersistentTopic(topicName, messages, 0);
    }

    private void publishMessagesOnPersistentTopic(String topicName, int messages, int startIdx) throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        for (int i = startIdx; i < (messages + startIdx); i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        producer.close();
    }



    private static class IncompatiblePropertyAdmin {
        public Set<String> allowedClusters;
        public int someNewIntField;
        public String someNewString;
    }


    @Value
    @Builder
    static class CustomTenantAdmin implements TenantInfo {
        private final int newTenant;
        private final Set<String> adminRoles;
        private final Set<String> allowedClusters;
    }



    static class MockedPulsarService extends MockedPulsarServiceBaseTest {

        private final ServiceConfiguration conf;

        public MockedPulsarService(ServiceConfiguration conf) {
            super();
            this.conf = conf;
        }

        @Override
        protected void setup() throws Exception {
            super.conf.setLoadManagerClassName(conf.getLoadManagerClassName());
            super.internalSetup();
        }

        @Override
        protected void cleanup() throws Exception {
            super.internalCleanup();
        }

        public PulsarService getPulsar() {
            return pulsar;
        }

        public PulsarAdmin getAdmin() {
            return admin;
        }
    }

    private void clearCache() {
        ((MetadataCacheImpl) pulsar.getPulsarResources().getNamespaceResources().getCache()).invalidateAll();
    }
}
