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

package org.apache.pulsar.broker.loadbalance.extensions.channel;

import static com.google.common.base.Preconditions.checkArgument;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.impl.MetadataStoreTableView;

@Slf4j
public class ServiceUnitStateMetadataStoreTableViewImpl implements ServiceUnitStateTableView {
    private PulsarService pulsar;
    private MetadataStoreTableView<ServiceUnitStateData> tableview;
    private BiConsumer<String, ServiceUnitStateData> tailMsgListener;
    private BiConsumer<String, ServiceUnitStateData> existingMsgListener;
    private BiConsumer<String, ServiceUnitStateData> skipMsgListener;
    private ServiceUnitStateCompactionStrategy conflictResolver;


    public void init(PulsarService pulsar,
                BiConsumer<String, ServiceUnitStateData> tailMsgListener,
                BiConsumer<String, ServiceUnitStateData> existingMsgListener,
                BiConsumer<String, ServiceUnitStateData> skipMsgListener) {
        this.pulsar = pulsar;
        this.tailMsgListener = tailMsgListener;
        this.existingMsgListener = existingMsgListener;
        this.skipMsgListener = skipMsgListener;
        conflictResolver = new ServiceUnitStateCompactionStrategy();
        conflictResolver.setSkippedMsgHandler(skipMsgListener);
    }

    public void start() throws Exception {

        boolean debug = ExtensibleLoadManagerImpl.debug(pulsar.getConfiguration(), log);

        if (tableview != null) {
            tableview.close();
            if (debug) {
                log.info("Closed the channel tableview.");
            }
        }

        tableview = new MetadataStoreTableView<>(ServiceUnitStateData.class,
                pulsar.getConfiguration().getMetadataStoreUrl(),
                MetadataStoreConfig.builder().build(),
                this::filterServiceUnitKey,
                this::resolveConflict,
                List.of(tailMsgListener),
                List.of(existingMsgListener),
                pulsar.getConfiguration().getMetadataStoreOperationTimeoutSeconds());
    }

    boolean filterServiceUnitKey(String key) {
        try {
            int i = key.lastIndexOf('/');
            checkArgument(i >= 0, "Invalid bundle format:" + key);
            String range = key.substring(i + 1);
            checkArgument(range.contains("_"), "Invalid bundle range:" + range);
            String[] boundaries = range.split("_");
            checkArgument(boundaries.length == 2, "Invalid boundaries:" + range);
            Long.decode(boundaries[0]);
            Long.decode(boundaries[1]);
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    protected boolean resolveConflict(ServiceUnitStateData prev, ServiceUnitStateData cur) {
        return !conflictResolver.shouldKeepLeft(prev, cur);
    }


    @Override
    public void close() throws IOException {

        boolean debug = ExtensibleLoadManagerImpl.debug(pulsar.getConfiguration(), log);
        if (tableview != null) {
            tableview.close();
            tableview = null;
            if (debug) {
                log.info("Successfully closed the channel tableview.");
            }
        }
    }

    @Override
    public int size() {
        return tableview.size();
    }

    @Override
    public boolean isEmpty() {
        return tableview.isEmpty();
    }

    @Override
    public ServiceUnitStateData get(String key) {
        return tableview.get(key);
    }

    @Override
    public CompletableFuture<Void> put(String key, ServiceUnitStateData value) {
        return tableview.put(key, value);
    }

    @Override
    public void flush(long waitDurationInMillis) throws Exception {
        // no-op
    }

    @Override
    public CompletableFuture<Void> remove(String key) {
        return tableview.remove(key);
    }

    @Override
    public Set<Map.Entry<String, ServiceUnitStateData>> entrySet() {
        return tableview.entrySet();
    }

    @Override
    public Collection<ServiceUnitStateData> values() {
        return tableview.values();
    }
}
