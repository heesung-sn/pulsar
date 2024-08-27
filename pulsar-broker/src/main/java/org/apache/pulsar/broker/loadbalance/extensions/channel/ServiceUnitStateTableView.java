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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;

/**
 * Defines the ServiceUnitStateTableView interface.
 */
public interface ServiceUnitStateTableView extends Closeable {

    void init(PulsarService pulsar,
              BiConsumer<String, ServiceUnitStateData> tailMsgListener,
              BiConsumer<String, ServiceUnitStateData> existingMsgListener,
              BiConsumer<String, ServiceUnitStateData> skipMsgListener);

    /**
     * Starts the ServiceUnitStateTableView.
     * @throws PulsarServerException if it fails to start the channel.
     */
    void start() throws Exception;

    /**
     * Closes the ServiceUnitStateTableView.
     * @throws PulsarServerException if it fails to close the channel.
     */
    void close() throws IOException;

    int size();
    boolean isEmpty();
    ServiceUnitStateData get(String key);
    CompletableFuture<Void> put(String key, ServiceUnitStateData value);

    void flush(long waitDurationInMillis) throws Exception;
    CompletableFuture<Void> remove(String key);
    Set<Map.Entry<String, ServiceUnitStateData>> entrySet();
    Collection<ServiceUnitStateData> values();
}
