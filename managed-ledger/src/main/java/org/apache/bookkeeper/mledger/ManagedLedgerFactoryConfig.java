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
package org.apache.bookkeeper.mledger;

import lombok.Data;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;

/**
 * Configuration for a {@link ManagedLedgerFactory}.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
@Data
public class ManagedLedgerFactoryConfig {
    private static final long MB = 1024 * 1024;

    private long maxCacheSize = 128 * MB;

    /**
     * The cache eviction watermark is the percentage of the cache size to reach when removing entries from the cache.
     */
    private double cacheEvictionWatermark = 0.90;

    private int numManagedLedgerSchedulerThreads = Runtime.getRuntime().availableProcessors();

    /**
     * Interval of cache eviction triggering. Default is 10 ms times.
     */
    private long cacheEvictionIntervalMs = 10;

    /**
     * All entries that have stayed in cache for more than the configured time, will be evicted.
     */
    private long cacheEvictionTimeThresholdMillis = 1000;

    /**
     * Whether we should make a copy of the entry payloads when inserting in cache.
     */
    private boolean copyEntriesInCache = false;

    /**
     * Maximum number of (estimated) data in-flight reading from storage and the cache.
     */
    private long managedLedgerMaxReadsInFlightSize = 0;

    /**
     * Maximum time to wait for acquiring permits for max reads in flight when managedLedgerMaxReadsInFlightSizeInMB is
     * set (>0) and the limit is reached.
     */
    private long managedLedgerMaxReadsInFlightPermitsAcquireTimeoutMillis = 60000;

    /**
     * Maximum number of reads that can be queued for acquiring permits for max reads in flight when
     * managedLedgerMaxReadsInFlightSizeInMB is set (>0) and the limit is reached.
     */
    private int managedLedgerMaxReadsInFlightPermitsAcquireQueueSize = 10000;

    /**
     * Whether trace managed ledger task execution time.
     */
    private boolean traceTaskExecution = true;

    /**
     * Managed ledger prometheus stats Latency Rollover Seconds.
     */
    private int prometheusStatsLatencyRolloverSeconds = 60;

    /**
     * How frequently to flush the cursor positions that were accumulated due to rate limiting.
     */
    private int cursorPositionFlushSeconds = 60;

    /**
     * How frequently to refresh the stats.
     */
    private int statsPeriodSeconds = 60;

    /**
     * cluster name for prometheus stats.
     */
    private String clusterName;

    /**
     * ManagedLedgerInfo compression type. If the compression type is null or invalid, don't compress data.
     */
    private String managedLedgerInfoCompressionType = MLDataFormats.CompressionType.NONE.name();

    /**
     * ManagedLedgerInfo compression threshold. If the origin metadata size below configuration.
     * compression will not apply.
     */
    private long managedLedgerInfoCompressionThresholdInBytes = 0;

    /**
     * ManagedCursorInfo compression type. If the compression type is null or invalid, don't compress data.
     */
    private String managedCursorInfoCompressionType = MLDataFormats.CompressionType.NONE.name();

    /**
     * ManagedCursorInfo compression threshold. If the origin metadata size below configuration.
     * compression will not apply.
     */
    private long managedCursorInfoCompressionThresholdInBytes = 0;

    public MetadataCompressionConfig getCompressionConfigForManagedLedgerInfo() {
        return new MetadataCompressionConfig(managedLedgerInfoCompressionType,
                managedLedgerInfoCompressionThresholdInBytes);
    }

    public MetadataCompressionConfig getCompressionConfigForManagedCursorInfo() {
        return new MetadataCompressionConfig(managedCursorInfoCompressionType,
                managedCursorInfoCompressionThresholdInBytes);
    }
}
