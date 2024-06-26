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
package com.nydia.agent.log4j;

import com.nydia.agent.log4j.message.StructuredDataMessage;
import com.nydia.agent.log4j.spi.ExtendedLogger;

/**
 *  Logs "Events" that are represented as {@link StructuredDataMessage}.
 */
public final class EventLogger {

    /**
     * Defines the Event Marker.
     */
    public static final Marker EVENT_MARKER = MarkerManager.getMarker("EVENT");

    private static final String NAME = "EventLogger";

    private static final String FQCN = EventLogger.class.getName();

    private static final ExtendedLogger LOGGER = LogManager.getContext(false).getLogger(NAME);

    private EventLogger() {
        // empty
    }

    /**
     * Logs events with a level of ALL.
     * @param msg The event StructuredDataMessage.
     */
    public static void logEvent(final StructuredDataMessage msg) {
        LOGGER.logIfEnabled(FQCN, Level.OFF, EVENT_MARKER, msg, null);
    }

    /**
     * Logs events and specify the logging level.
     * @param msg The event StructuredDataMessage.
     * @param level The logging Level.
     */
    public static void logEvent(final StructuredDataMessage msg, final Level level) {
        LOGGER.logIfEnabled(FQCN, level, EVENT_MARKER, msg, null);
    }
}
