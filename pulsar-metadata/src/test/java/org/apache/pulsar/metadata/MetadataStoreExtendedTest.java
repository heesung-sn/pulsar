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
package org.apache.pulsar.metadata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.EnumSet;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.testng.annotations.Test;

public class MetadataStoreExtendedTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl")
    public void sequentialKeys(String provider, Supplier<String> urlSupplier) throws Exception {
        final String basePath = newKey();

        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        Stat stat1 = store.put(basePath, "value-1".getBytes(), Optional.of(-1L), EnumSet.of(CreateOption.Sequential))
                .join();
        assertNotNull(stat1);
        assertTrue(stat1.getVersion() >= 0L);
        assertTrue(stat1.isFirstVersion());
        assertNotEquals(stat1.getPath(), basePath);
        assertEquals(store.get(stat1.getPath()).join().get().getValue(), "value-1".getBytes());
        String seq1 = stat1.getPath().replace(basePath, "");
        long n1 = Long.parseLong(seq1);

        Stat stat2 = store.put(basePath, "value-2".getBytes(), Optional.of(-1L), EnumSet.of(CreateOption.Sequential))
                .join();
        assertNotNull(stat2);
        assertTrue(stat2.getVersion() >= 0L);
        assertTrue(stat2.isFirstVersion());
        assertNotEquals(stat2.getPath(), basePath);
        assertNotEquals(stat2.getPath(), stat1.getPath());
        assertEquals(store.get(stat2.getPath()).join().get().getValue(), "value-2".getBytes());
        String seq2 = stat2.getPath().replace(basePath, "");
        long n2 = Long.parseLong(seq2);

        assertNotEquals(seq1, seq2);
        assertTrue(n1 < n2);
    }

    @Test(dataProvider = "impl", invocationCount = 100)
    public void readYourWrites(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());
        assertTrue(store instanceof ZKMetadataStore);
        MetadataCache<MetadataCacheTest.MyClass> objCache = store.getMetadataCache(MetadataCacheTest.MyClass.class);

        String key1 = "/mytest/key-1";
        String key2 = "/mytest/key-2";
        try {
            assertEquals(objCache.getIfCached(key1), Optional.empty());
            assertEquals(objCache.get(key1).join(), Optional.empty());

            MetadataCacheTest.MyClass value1 = new MetadataCacheTest.MyClass("a", 1);
            store.put(key1, ObjectMapperFactory.getMapper().writer().writeValueAsBytes(value1), Optional.of(-1L)).join();


            MetadataCacheTest.MyClass value2 = new MetadataCacheTest.MyClass("b", 2);
            store.put(key2, ObjectMapperFactory.getMapper().writer().writeValueAsBytes(value2), Optional.of(-1L)).join();

            assertEquals(objCache.get(key1).join(), Optional.of(value1));
            assertEqualsAndRetry(() -> objCache.getIfCached(key1), Optional.of(value1), Optional.empty());

            assertEquals(objCache.get(key2).join(), Optional.of(value2));
            assertEqualsAndRetry(() -> objCache.getIfCached(key2), Optional.of(value2), Optional.empty());

            objCache.delete(key2).join();

            assertEquals(objCache.get(key2).join(), Optional.empty());
            System.out.println("###:" + store.getChildren("/mytest").get());
            assertEquals(store.getChildren("/mytest").get().size(), 1);
        } finally {
            store.deleteIfExists(key1, Optional.of(-1L)).join();
            store.deleteIfExists(key2, Optional.of(-1L)).join();
        }

    }
}
