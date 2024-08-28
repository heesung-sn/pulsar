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

package org.apache.pulsar.metadata.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.NotificationType;

@Slf4j
public class MetadataStoreTableView<T> {
    @Getter
    private final MetadataStore store;
    private final MetadataCache<T> cache;
    private final BiPredicate<T, T> conflictResolver;
    private final List<BiConsumer<String, T>> tailMsgListeners;
    private final List<BiConsumer<String, T>> existingMsgListeners;
    private long timeoutDurationInSecs;
    private String pathPrefix;


    public MetadataStoreTableView(Class<T> clazz,
                                  @NonNull String serviceAddress,
                                  MetadataStoreConfig metadataStoreConfig,
                                  String pathPrefix,
                                  BiPredicate<T, T>  conflictResolver,
                                  List<BiConsumer<String, T>> tailMsgListeners,
                                  List<BiConsumer<String, T>> existingMsgListeners,
                                  long timeoutDurationInSecs)
            throws Exception {
        this.store = MetadataStoreFactoryImpl.create(serviceAddress, metadataStoreConfig);
        this.cache = store.getMetadataCache(clazz);
        this.timeoutDurationInSecs = timeoutDurationInSecs;

        store.registerListener(this::handleNotification);
        this.pathPrefix = pathPrefix;
        this.conflictResolver = conflictResolver;
        this.tailMsgListeners = new ArrayList<>();
        if (tailMsgListeners != null) {
            this.tailMsgListeners.addAll(tailMsgListeners);
        }

        this.existingMsgListeners = new ArrayList<>();
        if (existingMsgListeners != null) {
            this.existingMsgListeners.addAll(existingMsgListeners);
        }



    }

    private void handleNotification(org.apache.pulsar.metadata.api.Notification notification) {

        if (notification.getType() == NotificationType.ChildrenChanged) {
            return;
        }

        String path = notification.getPath();
        if (!path.startsWith(pathPrefix)) {
            return;
        }

        cache.get(path).thenAccept(valOpt -> {
            var val = valOpt.orElse(null);
            for (var listener : tailMsgListeners) {
                try {
                    String key = path.replaceFirst(getPath(""), "");
                    listener.accept(key, val);
                } catch (Throwable e) {
                    log.error("Failed to listen tail msg path:{}, val:{}", path, val, e);
                }
            }
        });
    }

    public void start() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        store.getChildren(pathPrefix)
                .thenAccept(keys -> {
            for (var key : keys) {
                // filter keys for this table view
                if (!key.startsWith(pathPrefix)) {
                    continue;
                }
                try {
                    cache.get(key).thenAccept(valOpt -> {
                        valOpt.ifPresent(val -> {
                            var listeners =
                                    existingMsgListeners.isEmpty() ? tailMsgListeners : existingMsgListeners;
                            for (var listener : listeners) {
                                try {
                                    listener.accept(key, val);
                                } catch (Throwable e) {
                                    future.completeExceptionally(e);
                                    log.error("Failed to listen existing msg key:{}, val:{}", key, val, e);
                                    throw e;
                                }
                            }
                        });
                    });
                } catch (Throwable e) {
                    if (!future.isCompletedExceptionally()) {
                        future.completeExceptionally(e);
                    }
                    log.error("Failed to handle key:{}", key, e);
                    break;
                }
            }

            future.complete(null);
        });

        future.join();
    }

    private String getPath(String key) {
        return pathPrefix + "/" + key;
    }

    public int size() {
        return cache.asMap(getPath("")).size();
    }

    public boolean isEmpty() {
        return cache.asMap(getPath("")).isEmpty();
    }

    public CompletableFuture<Boolean> containsKey(String key) {
        return cache.exists(getPath(key));
    }

    public CompletableFuture<Optional<T>> getAsync(String key) {
        return cache.get(getPath(key));
    }

    public T get(String key) {
        try {
            return cache.get(getPath(key)).get(timeoutDurationInSecs, TimeUnit.SECONDS).orElse(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<Void> put(String key, T value) {
        log.info("putting k:{}, v:{}", key, value);
        return cache.readModifyUpdateOrCreate(getPath(key), (old) -> {
            if (old.isEmpty()) {
                return value;
            } else {
                if (conflictResolver.test(old.get(), value)) {
                    return value;
                } else {
                    throw new IllegalStateException(
                            String.format("Failed to update from old:%s to value:%s", old, value));
                }
            }
        }).thenApply(__ -> null);
    }

    public CompletableFuture<Void> remove(String key) {
        log.info("removing k:{}", key);
        return cache.delete(getPath(key))
        .thenApply(__ -> null);
    }



    public Set<Map.Entry<String, T>> entrySet() {
        return cache.asMap(getPath("")).entrySet();
    }

    public Set<String> keySet() {
        return cache.asMap(getPath("")).keySet();
    }

    public Collection<T> values() {
        return cache.asMap(getPath("")).values();
    }

    public void forEach(BiConsumer<String, T> action) {
        cache.asMap(getPath("")).forEach(action);
    }

    @SneakyThrows
    public void close(){
        store.close();
    }


}
