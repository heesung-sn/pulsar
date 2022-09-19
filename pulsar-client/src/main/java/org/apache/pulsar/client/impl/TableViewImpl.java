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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;

@Slf4j
public class TableViewImpl<T> implements TableView<T> {

    private final TableViewConfigurationData conf;

    private final ConcurrentMap<String, T> data;
    private final Map<String, T> immutableData;

    private CompletableFuture<Reader<T>> readerFuture;

    private Reader<T> reader;

    private final List<BiConsumer<String, T>> listeners;
    private final ReentrantLock listenersMutex;
    private volatile boolean isActive;

    private MessageId lastMessageId;

    private PulsarClientImpl client;

    private Schema<T> schema;

    private Backoff recoveryBackoff;

    TableViewImpl(PulsarClientImpl client, Schema<T> schema, TableViewConfigurationData conf) {
        this.client = client;
        this.schema = schema;
        this.conf = conf;
        this.data = new ConcurrentHashMap<>();
        this.immutableData = Collections.unmodifiableMap(data);
        this.listeners = new ArrayList<>();
        this.listenersMutex = new ReentrantLock();
        this.recoveryBackoff = new Backoff(100, TimeUnit.MILLISECONDS,
                1, TimeUnit.MINUTES, 0, TimeUnit.MILLISECONDS);
        this.readerFuture = createReaderAsync(MessageId.earliest);

    }

    CompletableFuture<TableView<T>> start() {
        return readerFuture.thenCompose(this::readAllExistingMessages)
                .thenApply(__ -> this);
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public boolean isEmpty() {
        return data.isEmpty();
    }

    @Override
    public boolean containsKey(String key) {
        return data.containsKey(key);
    }

    @Override
    public T get(String key) {
       return data.get(key);
    }

    @Override
    public Set<Map.Entry<String, T>> entrySet() {
       return immutableData.entrySet();
    }

    @Override
    public Set<String> keySet() {
        return immutableData.keySet();
    }

    @Override
    public Collection<T> values() {
        return immutableData.values();
    }

    @Override
    public void forEach(BiConsumer<String, T> action) {
        data.forEach(action);
    }

    @Override
    public void forEachAndListen(BiConsumer<String, T> action) {
        // Ensure we iterate over all the existing entry _and_ start the listening from the exact next message
        try {
            listenersMutex.lock();

            // Execute the action over existing entries
            forEach(action);

            listeners.add(action);
        } finally {
            listenersMutex.unlock();
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return readerFuture.thenCompose(Reader::closeAsync);
    }

    @Override
    public void close() throws PulsarClientException {
        try {
            closeAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    private void handleMessage(Message<T> msg) {
        try {
            if (msg.hasKey()) {
                if (log.isDebugEnabled()) {
                    log.debug("Applying message from topic {}. key={} value={}",
                            conf.getTopicName(),
                            msg.getKey(),
                            msg.getValue());
                }

                try {
                    listenersMutex.lock();
                    lastMessageId = msg.getMessageId();
                    if (null == msg.getValue()){
                        data.remove(msg.getKey());
                    } else {
                        data.put(msg.getKey(), msg.getValue());
                    }

                    for (BiConsumer<String, T> listener : listeners) {
                        try {
                            listener.accept(msg.getKey(), msg.getValue());
                        } catch (Throwable t) {
                            log.error("Table view listener raised an exception", t);
                        }
                    }
                } finally {
                    listenersMutex.unlock();
                }
            }
        } finally {
            msg.release();
        }
    }

    private CompletableFuture<Reader<T>> readAllExistingMessages(Reader<T> reader) {
        long startTime = System.nanoTime();
        AtomicLong messagesRead = new AtomicLong();
        CompletableFuture<Reader<T>> future = new CompletableFuture<>();
        readAllExistingMessages(future, startTime, messagesRead);
        return future;
    }

    private void readAllExistingMessages(CompletableFuture<Reader<T>> future, long startTime,
                                         AtomicLong messagesRead) {
        reader.hasMessageAvailableAsync()
                .thenAccept(hasMessage -> {
                   if (hasMessage) {
                       reader.readNextAsync()
                               .thenAccept(msg -> {
                                   messagesRead.incrementAndGet();
                                   handleMessage(msg);
                                   readAllExistingMessages(future, startTime, messagesRead);
                               }).exceptionally(ex -> {
                                   if (ex.getCause() instanceof PulsarClientException.AlreadyClosedException) {
                                       log.warn("Reader {} was interrupted. {}", reader.getTopic(),
                                               ex.getMessage());
                                   } else {
                                       log.error("Reader {} was interrupted.", reader.getTopic(), ex);
                                   }
                                   isActive = false;
                                   future.completeExceptionally(ex);
                                   if (conf.isFaultTolerant()) {
                                       recover();
                                   }
                                   return null;
                               });
                   } else {
                       // Reached the end
                       long endTime = System.nanoTime();
                       long durationMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
                       log.info("Started table view for topic {} - Replayed {} messages in {} seconds",
                               reader.getTopic(),
                               messagesRead,
                               durationMillis / 1000.0);
                       future.complete(reader);
                       readTailMessages();
                   }
                });
    }

    private CompletableFuture<Reader<T>> createReaderAsync(MessageId startMessageId) {
        return client.newReader(schema)
                .topic(conf.getTopicName())
                .startMessageId(startMessageId)
                .autoUpdatePartitions(true)
                .autoUpdatePartitionsInterval((int) conf.getAutoUpdatePartitionsSeconds(), TimeUnit.SECONDS)
                .readCompacted(true)
                .poolMessages(true)
                .createAsync()
                .thenApply((reader) -> {
                    isActive = true;
                    this.reader = reader;
                    recoveryBackoff.reset();
                    return reader;
                });
    }

    private void recover() {
        try {
            Thread.sleep(recoveryBackoff.getNext());
        } catch (InterruptedException e) {
            log.debug("Interrupted sleep", e);
        }
        MessageId startMessageId = lastMessageId == null ? MessageId.earliest : lastMessageId;
        if (reader != null) {
            this.readerFuture = reader
                    .closeAsync()
                    .thenCompose((__) -> createReaderAsync(startMessageId));
        } else {
            this.readerFuture = createReaderAsync(startMessageId);
        }
        start();
    }

    private void readTailMessages() {
        reader.readNextAsync()
                .thenAccept(msg -> {
                    handleMessage(msg);
                    readTailMessages();
                }).exceptionally(ex -> {
                    isActive = false;
                    if (ex.getCause() instanceof PulsarClientException.AlreadyClosedException) {
                        log.warn("Reader {} was interrupted. {}", reader.getTopic(), ex.getMessage());
                    } else {
                        log.error("Reader {} was interrupted.", reader.getTopic(), ex);
                    }
                    if (conf.isFaultTolerant()) {
                        recover();
                    }
                    return null;
                });
    }

    public boolean isActive() {
        return isActive;
    }
}
