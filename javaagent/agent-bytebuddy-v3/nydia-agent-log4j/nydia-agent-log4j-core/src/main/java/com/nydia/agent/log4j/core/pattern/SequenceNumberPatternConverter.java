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
package com.nydia.agent.log4j.core.pattern;

import com.nydia.agent.log4j.core.LogEvent;
import com.nydia.agent.log4j.core.config.plugins.Plugin;
import com.nydia.agent.log4j.util.PerformanceSensitive;

import java.util.concurrent.atomic.AtomicLong;


/**
 * Formats the event sequence number.
 */
@Plugin(name = "SequenceNumberPatternConverter", category = "Converter")
@ConverterKeys({ "sn", "sequenceNumber" })
@PerformanceSensitive("allocation")
public final class SequenceNumberPatternConverter extends LogEventPatternConverter {

    private static final AtomicLong SEQUENCE = new AtomicLong();

    /**
     * Singleton.
     */
    private static final SequenceNumberPatternConverter INSTANCE =
        new SequenceNumberPatternConverter();

    /**
     * Private constructor.
     */
    private SequenceNumberPatternConverter() {
        super("Sequence Number", "sn");
    }

    /**
     * Obtains an instance of SequencePatternConverter.
     *
     * @param options options, currently ignored, may be null.
     * @return instance of SequencePatternConverter.
     */
    public static SequenceNumberPatternConverter newInstance(final String[] options) {
        return INSTANCE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void format(final LogEvent event, final StringBuilder toAppendTo) {
        toAppendTo.append(SEQUENCE.incrementAndGet());
    }
}
