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
package com.nydia.agent.log4j.core.appender;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.zip.Deflater;
import com.nydia.agent.log4j.core.Appender;
import com.nydia.agent.log4j.core.Core;
import com.nydia.agent.log4j.core.Filter;
import com.nydia.agent.log4j.core.Layout;
import com.nydia.agent.log4j.core.LogEvent;
import com.nydia.agent.log4j.core.appender.rolling.DefaultRolloverStrategy;
import com.nydia.agent.log4j.core.appender.rolling.DirectFileRolloverStrategy;
import com.nydia.agent.log4j.core.appender.rolling.DirectWriteRolloverStrategy;
import com.nydia.agent.log4j.core.appender.rolling.RollingFileManager;
import com.nydia.agent.log4j.core.appender.rolling.RolloverStrategy;
import com.nydia.agent.log4j.core.appender.rolling.TriggeringPolicy;
import com.nydia.agent.log4j.core.config.Configuration;
import com.nydia.agent.log4j.core.config.plugins.Plugin;
import com.nydia.agent.log4j.core.config.plugins.PluginBuilderAttribute;
import com.nydia.agent.log4j.core.config.plugins.PluginBuilderFactory;
import com.nydia.agent.log4j.core.config.plugins.PluginElement;
import com.nydia.agent.log4j.core.config.plugins.validation.constraints.Required;
import com.nydia.agent.log4j.core.util.Booleans;
import com.nydia.agent.log4j.core.util.Integers;
import com.nydia.agent.log4j.core.appender.rolling.*;

/**
 * An appender that writes to files and can roll over at intervals.
 */
@Plugin(name = RollingFileAppender.PLUGIN_NAME, category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public final class RollingFileAppender extends AbstractOutputStreamAppender<RollingFileManager> {

    public static final String PLUGIN_NAME = "RollingFile";

    /**
     * Builds FileAppender instances.
     * 
     * @param <B>
     *            The type to build
     */
    public static class Builder<B extends Builder<B>> extends AbstractOutputStreamAppender.Builder<B>
            implements com.nydia.agent.log4j.core.util.Builder<RollingFileAppender> {

        @PluginBuilderAttribute
        private String fileName;

        @PluginBuilderAttribute
        @Required
        private String filePattern;

        @PluginBuilderAttribute
        private boolean append = true;

        @PluginBuilderAttribute
        private boolean locking;

        @PluginElement("Policy") 
        @Required
        private TriggeringPolicy policy;
        
        @PluginElement("Strategy") 
        private RolloverStrategy strategy;

        @PluginBuilderAttribute
        private boolean advertise;

        @PluginBuilderAttribute
        private String advertiseUri;

        @PluginBuilderAttribute
        private boolean createOnDemand;

        @PluginBuilderAttribute
        private String filePermissions;

        @PluginBuilderAttribute
        private String fileOwner;

        @PluginBuilderAttribute
        private String fileGroup;

        @Override
        public RollingFileAppender build() {
            // Even though some variables may be annotated with @Required, we must still perform validation here for
            // call sites that build builders programmatically.
            final boolean isBufferedIo = isBufferedIo();
            final int bufferSize = getBufferSize();
            if (getName() == null) {
                LOGGER.error("RollingFileAppender '{}': No name provided.", getName());
                return null;
            }

            if (!isBufferedIo && bufferSize > 0) {
                LOGGER.warn("RollingFileAppender '{}': The bufferSize is set to {} but bufferedIO is not true", getName(), bufferSize);
            }

            if (filePattern == null) {
                LOGGER.error("RollingFileAppender '{}': No file name pattern provided.", getName());
                return null;
            }

            if (policy == null) {
                LOGGER.error("RollingFileAppender '{}': No TriggeringPolicy provided.", getName());
                return null;
            }

            if (strategy == null) {
                if (fileName != null) {
                    strategy = DefaultRolloverStrategy.newBuilder()
                                        .withCompressionLevelStr(String.valueOf(Deflater.DEFAULT_COMPRESSION))
                                        .withConfig(getConfiguration())
                                        .build();
                } else {
                    strategy = DirectWriteRolloverStrategy.newBuilder()
                                        .withCompressionLevelStr(String.valueOf(Deflater.DEFAULT_COMPRESSION))
                                        .withConfig(getConfiguration())
                                        .build();
                }
            } else if (fileName == null && !(strategy instanceof DirectFileRolloverStrategy)) {
                LOGGER.error("RollingFileAppender '{}': When no file name is provided a DirectFilenameRolloverStrategy must be configured");
                return null;
            }

            final Layout<? extends Serializable> layout = getOrCreateLayout();
            final RollingFileManager manager = RollingFileManager.getFileManager(fileName, filePattern, append,
                    isBufferedIo, policy, strategy, advertiseUri, layout, bufferSize, isImmediateFlush(),
                    createOnDemand, filePermissions, fileOwner, fileGroup, getConfiguration());
            if (manager == null) {
                return null;
            }

            manager.initialize();

            return new RollingFileAppender(getName(), layout, getFilter(), manager, fileName, filePattern,
                    isIgnoreExceptions(), isImmediateFlush());
        }

        public String getAdvertiseUri() {
            return advertiseUri;
        }

        public String getFileName() {
            return fileName;
        }

        public boolean isAdvertise() {
            return advertise;
        }

        public boolean isAppend() {
            return append;
        }

        public boolean isCreateOnDemand() {
            return createOnDemand;
        }

        public boolean isLocking() {
            return locking;
        }

        public String getFilePermissions() {
            return filePermissions;
        }

        public String getFileOwner() {
            return fileOwner;
        }

        public String getFileGroup() {
            return fileGroup;
        }

        public B withAdvertise(final boolean advertise) {
            this.advertise = advertise;
            return asBuilder();
        }

        public B withAdvertiseUri(final String advertiseUri) {
            this.advertiseUri = advertiseUri;
            return asBuilder();
        }

        public B withAppend(final boolean append) {
            this.append = append;
            return asBuilder();
        }

        public B withFileName(final String fileName) {
            this.fileName = fileName;
            return asBuilder();
        }

        public B withCreateOnDemand(final boolean createOnDemand) {
            this.createOnDemand = createOnDemand;
            return asBuilder();
        }

        public B withLocking(final boolean locking) {
            this.locking = locking;
            return asBuilder();
        }

        public String getFilePattern() {
            return filePattern;
        }

        public TriggeringPolicy getPolicy() {
            return policy;
        }

        public RolloverStrategy getStrategy() {
            return strategy;
        }

        public B withFilePattern(final String filePattern) {
            this.filePattern = filePattern;
            return asBuilder();
        }

        public B withPolicy(final TriggeringPolicy policy) {
            this.policy = policy;
            return asBuilder();
        }

        public B withStrategy(final RolloverStrategy strategy) {
            this.strategy = strategy;
            return asBuilder();
        }

        public B withFilePermissions(final String filePermissions) {
            this.filePermissions = filePermissions;
            return asBuilder();
        }

        public B withFileOwner(final String fileOwner) {
            this.fileOwner = fileOwner;
            return asBuilder();
        }

        public B withFileGroup(final String fileGroup) {
            this.fileGroup = fileGroup;
            return asBuilder();
        }

    }
    
    private static final int DEFAULT_BUFFER_SIZE = 8192;

    private final String fileName;
    private final String filePattern;
    private Object advertisement;

    private RollingFileAppender(final String name, final Layout<? extends Serializable> layout, final Filter filter,
            final RollingFileManager manager, final String fileName, final String filePattern,
            final boolean ignoreExceptions, final boolean immediateFlush) {
        super(name, layout, filter, ignoreExceptions, immediateFlush, manager);
        this.fileName = fileName;
        this.filePattern = filePattern;
    }

    @Override
    public boolean stop(final long timeout, final TimeUnit timeUnit) {
        setStopping();
        final boolean stopped = super.stop(timeout, timeUnit, false);
        setStopped();
        return stopped;
    }

    /**
     * Writes the log entry rolling over the file when required.

     * @param event The LogEvent.
     */
    @Override
    public void append(final LogEvent event) {
        getManager().checkRollover(event);
        super.append(event);
    }

    /**
     * Returns the File name for the Appender.
     * @return The file name.
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * Returns the file pattern used when rolling over.
     * @return The file pattern.
     */
    public String getFilePattern() {
        return filePattern;
    }

    /**
     * Returns the triggering policy.
     * @param <T> TriggeringPolicy type
     * @return The TriggeringPolicy
     */
    public <T extends TriggeringPolicy> T getTriggeringPolicy() {
        return getManager().getTriggeringPolicy();
    }

    /**
     * Creates a RollingFileAppender.
     * @param fileName The name of the file that is actively written to. (required).
     * @param filePattern The pattern of the file name to use on rollover. (required).
     * @param append If true, events are appended to the file. If false, the file
     * is overwritten when opened. Defaults to "true"
     * @param name The name of the Appender (required).
     * @param bufferedIO When true, I/O will be buffered. Defaults to "true".
     * @param bufferSizeStr buffer size for buffered IO (default is 8192).
     * @param immediateFlush When true, events are immediately flushed. Defaults to "true".
     * @param policy The triggering policy. (required).
     * @param strategy The rollover strategy. Defaults to DefaultRolloverStrategy.
     * @param layout The layout to use (defaults to the default PatternLayout).
     * @param filter The Filter or null.
     * @param ignore If {@code "true"} (default) exceptions encountered when appending events are logged; otherwise
     *               they are propagated to the caller.
     * @param advertise "true" if the appender configuration should be advertised, "false" otherwise.
     * @param advertiseUri The advertised URI which can be used to retrieve the file contents.
     * @param config The Configuration.
     * @return A RollingFileAppender.
     * @deprecated Use {@link #newBuilder()}.
     */
    @Deprecated
    public static <B extends Builder<B>> RollingFileAppender createAppender(
            // @formatter:off
            final String fileName,
            final String filePattern,
            final String append,
            final String name,
            final String bufferedIO,
            final String bufferSizeStr,
            final String immediateFlush,
            final TriggeringPolicy policy,
            final RolloverStrategy strategy,
            final Layout<? extends Serializable> layout,
            final Filter filter,
            final String ignore,
            final String advertise,
            final String advertiseUri,
            final Configuration config) {
            // @formatter:on
        final int bufferSize = Integers.parseInt(bufferSizeStr, DEFAULT_BUFFER_SIZE);
        // @formatter:off
        return RollingFileAppender.<B>newBuilder()
                .withAdvertise(Boolean.parseBoolean(advertise))
                .withAdvertiseUri(advertiseUri)
                .withAppend(Booleans.parseBoolean(append, true))
                .withBufferedIo(Booleans.parseBoolean(bufferedIO, true))
                .withBufferSize(bufferSize)
                .setConfiguration(config)
                .withFileName(fileName)
                .withFilePattern(filePattern)
                .withFilter(filter)
                .withIgnoreExceptions(Booleans.parseBoolean(ignore, true))
                .withImmediateFlush(Booleans.parseBoolean(immediateFlush, true))
                .withLayout(layout)
                .withCreateOnDemand(false)
                .withLocking(false)
                .withName(name)
                .withPolicy(policy)
                .withStrategy(strategy)
                .build();
        // @formatter:on
    }

    @PluginBuilderFactory
    public static <B extends Builder<B>> B newBuilder() {
        return new Builder<B>().asBuilder();
    }
}
