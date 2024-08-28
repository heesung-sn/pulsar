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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.pulsar.common.naming.NamespaceName.SYSTEM_NAMESPACE;
import static org.apache.pulsar.common.topics.TopicCompactionStrategy.TABLE_VIEW_TAG;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.topics.TopicCompactionStrategy;

@Slf4j
public class ServiceUnitStateTableViewImpl implements ServiceUnitStateTableView {

    public static final String TOPIC = TopicName.get(
            TopicDomain.persistent.value(),
            SYSTEM_NAMESPACE,
            "loadbalancer-service-unit-state").toString();
    private static final int MAX_OUTSTANDING_PUB_MESSAGES = 500;
    public static final CompressionType MSG_COMPRESSION_TYPE = CompressionType.ZSTD;
    private Producer<ServiceUnitStateData> producer;
    private TableView<ServiceUnitStateData> tableview;
    private PulsarService pulsar;
    private Schema<ServiceUnitStateData> schema;
    private BiConsumer<String, ServiceUnitStateData> tailMsgListener;
    private BiConsumer<String, ServiceUnitStateData> existingMsgListener;
    private BiConsumer<String, ServiceUnitStateData> skipMsgListener;


    public void init(PulsarService pulsar,
                     BiConsumer<String, ServiceUnitStateData> tailMsgListener,
                     BiConsumer<String, ServiceUnitStateData> existingMsgListener,
                     BiConsumer<String, ServiceUnitStateData> skipMsgListener) {
        this.pulsar = pulsar;
        this.schema = Schema.JSON(ServiceUnitStateData.class);
        this.tailMsgListener = tailMsgListener;
        this.existingMsgListener = existingMsgListener;
        this.skipMsgListener = skipMsgListener;
    }

    public void start() throws Exception {

        boolean debug = ExtensibleLoadManagerImpl.debug(pulsar.getConfiguration(), log);

        ExtensibleLoadManagerImpl.createSystemTopic(pulsar, TOPIC);

        if (producer != null) {
            producer.close();
            if (debug) {
                log.info("Closed the channel producer.");
            }
        }

        producer = pulsar.getClient().newProducer(schema)
                .enableBatching(true)
                .compressionType(MSG_COMPRESSION_TYPE)
                .maxPendingMessages(MAX_OUTSTANDING_PUB_MESSAGES)
                .blockIfQueueFull(true)
                .topic(TOPIC)
                .create();

        if (debug) {
            log.info("Successfully started the channel producer.");
        }

        if (tableview != null) {
            tableview.close();
            if (debug) {
                log.info("Closed the channel tableview.");
            }
        }


        tableview = pulsar.getClient().newTableViewBuilder(schema)
                .topic(TOPIC)
                .loadConf(Map.of(
                        "topicCompactionStrategyClassName",
                        ServiceUnitStateCompactionStrategy.class.getName()))
                .create();
        tableview.listen(tailMsgListener);
        tableview.forEach(existingMsgListener);

        var strategy = (ServiceUnitStateCompactionStrategy) TopicCompactionStrategy.getInstance(TABLE_VIEW_TAG);
        if (strategy == null) {
            String err = TABLE_VIEW_TAG + "tag TopicCompactionStrategy is null.";
            log.error(err);
            throw new IllegalStateException(err);
        }

        strategy.setSkippedMsgHandler(skipMsgListener);
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

        if (producer != null) {
            producer.close();
            producer = null;
            log.info("Successfully closed the channel producer.");
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
        CompletableFuture<Void> future = new CompletableFuture<>();
        producer.newMessage()
                .key(key)
                .value(value)
                .sendAsync()
                .whenComplete((messageId, e) -> {
                    if (e != null) {
                        log.error("Failed to publish the message: serviceUnit:{}, data:{}",
                                key, value, e);
                        future.completeExceptionally(e);
                    } else {
                        future.complete(null);
                    }
                });
        return future;
    }

    @Override
    public void flush(long waitDurationInMillis) throws Exception {
        producer.flushAsync().get(waitDurationInMillis, MILLISECONDS);
    }

    @Override
    public CompletableFuture<Void> remove(String key) {
        return put(key, null);
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
