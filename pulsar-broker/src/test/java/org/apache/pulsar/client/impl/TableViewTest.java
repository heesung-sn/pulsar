/**
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
package org.apache.pulsar.client.impl;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit test for {@link org.apache.pulsar.client.impl.TableViewImpl}.
 */
@Slf4j
@Test(groups = "broker-impl")
public class TableViewTest extends MockedPulsarServiceBaseTest {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setAllowAutoTopicCreation(true);
        super.internalSetup(conf);

        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        // so that clients can test short names
        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("public/default");
        admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private Set<String> publishMessages(String topic, int count, boolean enableBatch) throws Exception {
        Set<String> keys = new HashSet<>();
        ProducerBuilder<byte[]> builder = pulsarClient.newProducer();
        builder.messageRoutingMode(MessageRoutingMode.SinglePartition);
        builder.maxPendingMessages(count);
        // disable periodical flushing
        builder.batchingMaxPublishDelay(1, TimeUnit.DAYS);
        builder.topic(topic);
        if (enableBatch) {
            builder.enableBatching(true);
            builder.batchingMaxMessages(count);
        } else {
            builder.enableBatching(false);
        }
        try (Producer<byte[]> producer = builder.create()) {
            CompletableFuture<?> lastFuture = null;
            for (int i = 0; i < count; i++) {
                String key = "key"+ i;
                byte[] data = ("my-message-" + i).getBytes();
                lastFuture = producer.newMessage().key(key).value(data).sendAsync();
                keys.add(key);
            }
            producer.flush();
            lastFuture.get();
        }
        return keys;
    }

    @Test(timeOut = 30 * 1000)
    public void testTableView() throws Exception {
        String topic = "persistent://public/default/tableview-test";
        admin.topics().createPartitionedTopic(topic, 3);
        int count = 20;
        Set<String> keys = this.publishMessages(topic, count, false);
        @Cleanup
        TableView<byte[]> tv = pulsarClient.newTableViewBuilder(Schema.BYTES)
                .topic(topic)
                .autoUpdatePartitionsInterval(60, TimeUnit.SECONDS)
                .create();
        log.info("start tv size: {}", tv.size());
        tv.forEachAndListen((k, v) -> log.info("{} -> {}", k, new String(v)));
        Awaitility.await().untilAsserted(() -> {
            log.info("Current tv size: {}", tv.size());
            assertEquals(tv.size(), count);
        });
        assertEquals(tv.keySet(), keys);
        tv.forEachAndListen((k, v) -> log.info("checkpoint {} -> {}", k, new String(v)));

        // Send more data
        Set<String> keys2 = this.publishMessages(topic, count * 2, false);
        Awaitility.await().untilAsserted(() -> {
            log.info("Current tv size: {}", tv.size());
            assertEquals(tv.size(), count * 2);
        });
        assertEquals(tv.keySet(), keys2);
        // Test collection
        try {
            tv.keySet().clear();
            fail("Should fail here");
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof UnsupportedOperationException);
        }
        try {
            tv.entrySet().clear();
            fail("Should fail here");
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof UnsupportedOperationException);
        }
        try {
            tv.values().clear();
            fail("Should fail here");
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof UnsupportedOperationException);
        }
    }

    @Test(timeOut = 30 * 1000)
    public void testTableViewUpdatePartitions() throws Exception {
        String topic = "persistent://public/default/tableview-test-update-partitions";
        admin.topics().createPartitionedTopic(topic, 3);
        int count = 20;
        Set<String> keys = this.publishMessages(topic, count, false);
        @Cleanup
        TableView<byte[]> tv = pulsarClient.newTableViewBuilder(Schema.BYTES)
                .topic(topic)
                .autoUpdatePartitionsInterval(5, TimeUnit.SECONDS)
                .create();
        log.info("start tv size: {}", tv.size());
        tv.forEachAndListen((k, v) -> log.info("{} -> {}", k, new String(v)));
        Awaitility.await().untilAsserted(() -> {
            log.info("Current tv size: {}", tv.size());
            assertEquals(tv.size(), count);
        });
        assertEquals(tv.keySet(), keys);
        tv.forEachAndListen((k, v) -> log.info("checkpoint {} -> {}", k, new String(v)));

        admin.topics().updatePartitionedTopic(topic, 4);
        TopicName topicName = TopicName.get(topic);

        // Send more data to partition 3, which is not in the current TableView, need update partitions
        Set<String> keys2 =
                this.publishMessages(topicName.getPartition(3).toString(), count * 2, false);
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            log.info("Current tv size: {}", tv.size());
            assertEquals(tv.size(), count * 2);
        });
        assertEquals(tv.keySet(), keys2);
    }

    @Test(timeOut = 30 * 1000)
    public void testPublishNullValue() throws Exception {
        String topic = "persistent://public/default/tableview-test-publish-null-value";
        admin.topics().createPartitionedTopic(topic, 3);

        final TableView<String> tv = pulsarClient.newTableViewBuilder(Schema.STRING)
                .topic(topic)
                .autoUpdatePartitionsInterval(5, TimeUnit.SECONDS)
                .create();
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();

        producer.newMessage().key("key1").value("value1").send();

        Awaitility.await().untilAsserted(() -> assertEquals(tv.get("key1"), "value1"));
        assertEquals(tv.size(), 1);

        // Try to remove key1 by publishing the tombstones message.
        producer.newMessage().key("key1").value(null).send();
        Awaitility.await().untilAsserted(() -> assertEquals(tv.size(), 0));

        producer.newMessage().key("key2").value("value2").send();
        Awaitility.await().untilAsserted(() -> assertEquals(tv.get("key2"), "value2"));
        assertEquals(tv.size(), 1);

        tv.close();

        @Cleanup
        TableView<String> tv1 = pulsarClient.newTableViewBuilder(Schema.STRING)
                .topic(topic)
                .autoUpdatePartitionsInterval(5, TimeUnit.SECONDS)
                .create();

        assertEquals(tv1.size(), 1);
        assertEquals(tv.get("key2"), "value2");
    }

    @Test(timeOut = 30 * 1000)
    public void testInActive() throws Exception {
        String topic = "persistent://public/default/test";
        final TableView<String> tv = pulsarClient.newTableViewBuilder(Schema.STRING)
                .topic(topic)
                .autoUpdatePartitionsInterval(5, TimeUnit.SECONDS)
                .create();

        // Test if the tv is inactive when the reader is closed
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        assertTrue(tv.isActive());
        Reader<String> reader = (Reader<String>) FieldUtils.readField(tv, "reader", true);
        reader.close();
        producer.newMessage().key("key1").value("value1").send();
        tryToWaitForInActive(tv);
        assertTrue(!tv.isActive());

        // Test if the tv is inactive when the reader throws unexpected exceptions
        final TableView<String> tv2 = pulsarClient.newTableViewBuilder(Schema.STRING)
                .topic(topic)
                .autoUpdatePartitionsInterval(5, TimeUnit.SECONDS)
                .create();
        assertTrue(tv2.isActive());

        Reader<String> mockReader = mock(ReaderImpl.class, new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                throw new RuntimeException("mock error");
            }
        });

        FieldUtils.writeField(tv2, "reader", mockReader, true);
        producer.newMessage().key("key2").value("value2").send();
        tryToWaitForInActive(tv2);
        assertTrue(!tv2.isActive());

        // Test if the tv is active(recovered) when the table view is fault-tolerant and the reader is closed.
        final TableView<String> tv3 = pulsarClient.newTableViewBuilder(Schema.STRING)
                .topic(topic)
                .autoUpdatePartitionsInterval(5, TimeUnit.SECONDS)
                .faultTolerant(true)
                .create();
        ((Reader<String>) FieldUtils.readField(tv3, "reader", true)).close();
        MessageIdImpl messageId = (MessageIdImpl) FieldUtils.readField(tv3, "lastMessageId", true);
        producer.newMessage().key("key1").value("value3").send();
        MessageIdImpl messageId2 = tryToWaitForNewMessageId(tv3, messageId);
        assertEquals(messageId2.getEntryId(), messageId.getEntryId()+1);
        assertEquals(tv3.get("key1"), "value3");
        assertTrue(tv3.isActive());
    }

    private static void tryToWaitForInActive(TableView<String> tv) throws InterruptedException {
        int retry = 0;
        while (tv.isActive()) {
            Thread.sleep(250);
            if (retry++ > 5) {
                break;
            }
        }
    }

    private static MessageIdImpl tryToWaitForNewMessageId(TableView<String> tv, MessageIdImpl start)
            throws InterruptedException, IllegalAccessException {
        int retry = 0;
        MessageIdImpl messageId = start;
        while (messageId == start) {
            Thread.sleep(250);
            if (retry++ > 5) {
                break;
            }
            messageId = (MessageIdImpl) FieldUtils.readField(tv, "lastMessageId", true);
        }
        return messageId;
    }
}
