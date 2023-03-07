/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package com.hust.foolwc.dy.agent.log4j.core.async;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import com.hust.foolwc.dy.agent.log4j.Level;
import com.hust.foolwc.dy.agent.log4j.Logger;
import com.hust.foolwc.dy.agent.log4j.status.StatusLogger;

import com.hust.foolwc.dy.agent.log4j.Level;
import com.hust.foolwc.dy.agent.log4j.Logger;

/**
 * Discarding router extends the DefaultAsyncQueueFullPolicy by first verifying if the queue is fuller than the specified
 * threshold ratio; if this is the case, log events {@linkplain Level#isMoreSpecificThan(Level) more specific} than
 * the specified threshold level are dropped. If this is not the case, the {@linkplain DefaultAsyncQueueFullPolicy
 * default routing rules hold.
 */
public class DiscardingAsyncQueueFullPolicy extends DefaultAsyncQueueFullPolicy {
    private static final Logger LOGGER = StatusLogger.getLogger();

    private final Level thresholdLevel;
    private final AtomicLong discardCount = new AtomicLong();

    /**
     * Constructs a router that will discard events {@linkplain Level#isLessSpecificThan(Level) equal or less specific}
     * than the specified threshold level when the queue is full.
     *
     * @param thresholdLevel level of events to discard
     */
    public DiscardingAsyncQueueFullPolicy(final Level thresholdLevel) {
        this.thresholdLevel = Objects.requireNonNull(thresholdLevel, "thresholdLevel");
    }

    @Override
    public EventRoute getRoute(final long backgroundThreadId, final Level level) {
        if (level.isLessSpecificThan(thresholdLevel)) {
            if (discardCount.getAndIncrement() == 0) {
                LOGGER.warn("Async queue is full, discarding event with level {}. " +
                        "This message will only appear once; future events from {} " +
                        "are silently discarded until queue capacity becomes available.",
                        level, thresholdLevel);
            }
            return EventRoute.DISCARD;
        }
        return super.getRoute(backgroundThreadId, level);
    }

    public static long getDiscardCount(final AsyncQueueFullPolicy router) {
        if (router instanceof DiscardingAsyncQueueFullPolicy) {
            return ((DiscardingAsyncQueueFullPolicy) router).discardCount.get();
        }
        return 0;
    }

    public Level getThresholdLevel() {
        return thresholdLevel;
    }
}
