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
package com.nydia.agent.log4j.core.impl;

import com.nydia.agent.log4j.core.LifeCycle;
import com.nydia.agent.log4j.core.LoggerContext;
import com.nydia.agent.log4j.core.config.AbstractConfiguration;
import com.nydia.agent.log4j.core.config.Configuration;
import com.nydia.agent.log4j.core.config.ConfigurationFactory;
import com.nydia.agent.log4j.core.config.ConfigurationSource;
import com.nydia.agent.log4j.core.config.composite.CompositeConfiguration;
import com.nydia.agent.log4j.core.selector.ClassLoaderContextSelector;
import com.nydia.agent.log4j.core.selector.ContextSelector;
import com.nydia.agent.log4j.core.util.Cancellable;
import com.nydia.agent.log4j.core.util.Constants;
import com.nydia.agent.log4j.core.util.DefaultShutdownCallbackRegistry;
import com.nydia.agent.log4j.core.util.ShutdownCallbackRegistry;
import com.nydia.agent.log4j.spi.LoggerContextFactory;
import com.nydia.agent.log4j.status.StatusLogger;
import com.nydia.agent.log4j.util.LoaderUtil;
import com.nydia.agent.log4j.util.PropertiesUtil;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.nydia.agent.log4j.core.config.AbstractConfiguration;
import com.nydia.agent.log4j.core.config.Configuration;
import com.nydia.agent.log4j.core.config.ConfigurationFactory;
import com.nydia.agent.log4j.core.config.ConfigurationSource;
import com.nydia.agent.log4j.core.config.composite.CompositeConfiguration;
import com.nydia.agent.log4j.core.util.Cancellable;
import com.nydia.agent.log4j.core.util.Constants;
import com.nydia.agent.log4j.core.util.DefaultShutdownCallbackRegistry;
import com.nydia.agent.log4j.core.util.ShutdownCallbackRegistry;
import com.nydia.agent.log4j.spi.LoggerContextFactory;
import com.nydia.agent.log4j.core.config.AbstractConfiguration;
import com.nydia.agent.log4j.core.config.Configuration;
import com.nydia.agent.log4j.core.config.ConfigurationFactory;
import com.nydia.agent.log4j.core.config.ConfigurationSource;
import com.nydia.agent.log4j.core.config.composite.CompositeConfiguration;
import com.nydia.agent.log4j.core.selector.ClassLoaderContextSelector;
import com.nydia.agent.log4j.core.selector.ContextSelector;
import com.nydia.agent.log4j.core.util.Cancellable;
import com.nydia.agent.log4j.core.util.Constants;
import com.nydia.agent.log4j.core.util.DefaultShutdownCallbackRegistry;
import com.nydia.agent.log4j.core.util.ShutdownCallbackRegistry;
import com.nydia.agent.log4j.spi.LoggerContextFactory;


/**
 * Factory to locate a ContextSelector and then load a LoggerContext.
 */
public class Log4jContextFactory implements LoggerContextFactory, ShutdownCallbackRegistry {

    private static final StatusLogger LOGGER = StatusLogger.getLogger();
    private static final boolean SHUTDOWN_HOOK_ENABLED =
        PropertiesUtil.getProperties().getBooleanProperty(ShutdownCallbackRegistry.SHUTDOWN_HOOK_ENABLED, true);

    private final ContextSelector selector;
    private final ShutdownCallbackRegistry shutdownCallbackRegistry;

    /**
     * Initializes the ContextSelector from system property {@link Constants#LOG4J_CONTEXT_SELECTOR}.
     */
    public Log4jContextFactory() {
        this(createContextSelector(), createShutdownCallbackRegistry());
    }

    /**
     * Initializes this factory's ContextSelector with the specified selector.
     * @param selector the selector to use
     */
    public Log4jContextFactory(final ContextSelector selector) {
        this(selector, createShutdownCallbackRegistry());
    }

    /**
     * Constructs a Log4jContextFactory using the ContextSelector from {@link Constants#LOG4J_CONTEXT_SELECTOR}
     * and the provided ShutdownRegistrationStrategy.
     *
     * @param shutdownCallbackRegistry the ShutdownRegistrationStrategy to use
     * @since 2.1
     */
    public Log4jContextFactory(final ShutdownCallbackRegistry shutdownCallbackRegistry) {
        this(createContextSelector(), shutdownCallbackRegistry);
    }

    /**
     * Constructs a Log4jContextFactory using the provided ContextSelector and ShutdownRegistrationStrategy.
     *
     * @param selector                     the selector to use
     * @param shutdownCallbackRegistry the ShutdownRegistrationStrategy to use
     * @since 2.1
     */
    public Log4jContextFactory(final ContextSelector selector,
                               final ShutdownCallbackRegistry shutdownCallbackRegistry) {
        this.selector = Objects.requireNonNull(selector, "No ContextSelector provided");
        this.shutdownCallbackRegistry = Objects.requireNonNull(shutdownCallbackRegistry, "No ShutdownCallbackRegistry provided");
        LOGGER.debug("Using ShutdownCallbackRegistry {}", shutdownCallbackRegistry.getClass());
        initializeShutdownCallbackRegistry();
    }

    private static ContextSelector createContextSelector() {
        try {
            final ContextSelector selector = LoaderUtil.newCheckedInstanceOfProperty(Constants.LOG4J_CONTEXT_SELECTOR,
                ContextSelector.class);
            if (selector != null) {
                return selector;
            }
        } catch (final Exception e) {
            LOGGER.error("Unable to create custom ContextSelector. Falling back to default.", e);
        }
        return new ClassLoaderContextSelector();
    }

    private static ShutdownCallbackRegistry createShutdownCallbackRegistry() {
        try {
            final ShutdownCallbackRegistry registry = LoaderUtil.newCheckedInstanceOfProperty(
                ShutdownCallbackRegistry.SHUTDOWN_CALLBACK_REGISTRY, ShutdownCallbackRegistry.class
            );
            if (registry != null) {
                return registry;
            }
        } catch (final Exception e) {
            LOGGER.error("Unable to create custom ShutdownCallbackRegistry. Falling back to default.", e);
        }
        return new DefaultShutdownCallbackRegistry();
    }

    private void initializeShutdownCallbackRegistry() {
        if (SHUTDOWN_HOOK_ENABLED && this.shutdownCallbackRegistry instanceof LifeCycle) {
            try {
                ((LifeCycle) this.shutdownCallbackRegistry).start();
            } catch (final IllegalStateException e) {
                LOGGER.error("Cannot start ShutdownCallbackRegistry, already shutting down.");
                throw e;
            } catch (final RuntimeException e) {
                LOGGER.error("There was an error starting the ShutdownCallbackRegistry.", e);
            }
        }
    }

    /**
     * Loads the LoggerContext using the ContextSelector.
     * @param fqcn The fully qualified class name of the caller.
     * @param loader The ClassLoader to use or null.
     * @param currentContext If true returns the current Context, if false returns the Context appropriate
     * for the caller if a more appropriate Context can be determined.
     * @param externalContext An external context (such as a ServletContext) to be associated with the LoggerContext.
     * @return The LoggerContext.
     */
    @Override
    public LoggerContext getContext(final String fqcn, final ClassLoader loader, final Object externalContext,
                                    final boolean currentContext) {
        final LoggerContext ctx = selector.getContext(fqcn, loader, currentContext);
        if (externalContext != null && ctx.getExternalContext() == null) {
            ctx.setExternalContext(externalContext);
        }
        if (ctx.getState() == LifeCycle.State.INITIALIZED) {
            ctx.start();
        }
        return ctx;
    }

    /**
     * Loads the LoggerContext using the ContextSelector.
     * @param fqcn The fully qualified class name of the caller.
     * @param loader The ClassLoader to use or null.
     * @param externalContext An external context (such as a ServletContext) to be associated with the LoggerContext.
     * @param currentContext If true returns the current Context, if false returns the Context appropriate
     * for the caller if a more appropriate Context can be determined.
     * @param source The configuration source.
     * @return The LoggerContext.
     */
    public LoggerContext getContext(final String fqcn, final ClassLoader loader, final Object externalContext,
                                    final boolean currentContext, final ConfigurationSource source) {
        final LoggerContext ctx = selector.getContext(fqcn, loader, currentContext, null);
        if (externalContext != null && ctx.getExternalContext() == null) {
            ctx.setExternalContext(externalContext);
        }
        if (ctx.getState() == LifeCycle.State.INITIALIZED) {
            if (source != null) {
                ContextAnchor.THREAD_CONTEXT.set(ctx);
                final Configuration config = ConfigurationFactory.getInstance().getConfiguration(ctx, source);
                LOGGER.debug("Starting LoggerContext[name={}] from configuration {}", ctx.getName(), source);
                ctx.start(config);
                ContextAnchor.THREAD_CONTEXT.remove();
            } else {
                ctx.start();
            }
        }
        return ctx;
    }

    /**
     * Loads the LoggerContext using the ContextSelector using the provided Configuration
     * @param fqcn The fully qualified class name of the caller.
     * @param loader The ClassLoader to use or null.
     * @param externalContext An external context (such as a ServletContext) to be associated with the LoggerContext.
     * @param currentContext If true returns the current Context, if false returns the Context appropriate
     * for the caller if a more appropriate Context can be determined.
     * @param configuration The Configuration.
     * @return The LoggerContext.
     */
    public LoggerContext getContext(final String fqcn, final ClassLoader loader, final Object externalContext,
            final boolean currentContext, final Configuration configuration) {
        final LoggerContext ctx = selector.getContext(fqcn, loader, currentContext, null);
        if (externalContext != null && ctx.getExternalContext() == null) {
            ctx.setExternalContext(externalContext);
        }
        if (ctx.getState() == LifeCycle.State.INITIALIZED) {
            ContextAnchor.THREAD_CONTEXT.set(ctx);
            try {
                ctx.start(configuration);
            } finally {
                ContextAnchor.THREAD_CONTEXT.remove();
            }
        }
        return ctx;
    }

    /**
     * Loads the LoggerContext using the ContextSelector.
     * @param fqcn The fully qualified class name of the caller.
     * @param loader The ClassLoader to use or null.
     * @param externalContext An external context (such as a ServletContext) to be associated with the LoggerContext.
     * @param currentContext If true returns the current Context, if false returns the Context appropriate
     * for the caller if a more appropriate Context can be determined.
     * @param configLocation The location of the configuration for the LoggerContext (or null).
     * @return The LoggerContext.
     */
    @Override
    public LoggerContext getContext(final String fqcn, final ClassLoader loader, final Object externalContext,
                                    final boolean currentContext, final URI configLocation, final String name) {
        final LoggerContext ctx = selector.getContext(fqcn, loader, currentContext, configLocation);
        if (externalContext != null && ctx.getExternalContext() == null) {
            ctx.setExternalContext(externalContext);
        }
        if (name != null) {
        	ctx.setName(name);
        }
        if (ctx.getState() == LifeCycle.State.INITIALIZED) {
            if (configLocation != null || name != null) {
                ContextAnchor.THREAD_CONTEXT.set(ctx);
                final Configuration config = ConfigurationFactory.getInstance().getConfiguration(ctx, name, configLocation);
                LOGGER.debug("Starting LoggerContext[name={}] from configuration at {}", ctx.getName(), configLocation);
                ctx.start(config);
                ContextAnchor.THREAD_CONTEXT.remove();
            } else {
                ctx.start();
            }
        }
        return ctx;
    }

    public LoggerContext getContext(final String fqcn, final ClassLoader loader, final Object externalContext,
            final boolean currentContext, final List<URI> configLocations, final String name) {
        final LoggerContext ctx = selector
                .getContext(fqcn, loader, currentContext, null/*this probably needs to change*/);
        if (externalContext != null && ctx.getExternalContext() == null) {
            ctx.setExternalContext(externalContext);
        }
        if (name != null) {
            ctx.setName(name);
        }
        if (ctx.getState() == LifeCycle.State.INITIALIZED) {
            if ((configLocations != null && !configLocations.isEmpty())) {
                ContextAnchor.THREAD_CONTEXT.set(ctx);
                final List<AbstractConfiguration> configurations = new ArrayList<>(configLocations.size());
                for (final URI configLocation : configLocations) {
                    final Configuration currentReadConfiguration = ConfigurationFactory.getInstance()
                            .getConfiguration(ctx, name, configLocation);
                    if (currentReadConfiguration instanceof AbstractConfiguration) {
                        configurations.add((AbstractConfiguration) currentReadConfiguration);
                    } else {
                        LOGGER.error(
                                "Found configuration {}, which is not an AbstractConfiguration and can't be handled by CompositeConfiguration",
                                configLocation);
                    }
                }
                final CompositeConfiguration compositeConfiguration = new CompositeConfiguration(configurations);
                LOGGER.debug("Starting LoggerContext[name={}] from configurations at {}", ctx.getName(),
                        configLocations);
                ctx.start(compositeConfiguration);
                ContextAnchor.THREAD_CONTEXT.remove();
            } else {
                ctx.start();
            }
        }
        return ctx;
    }

    /**
     * Returns the ContextSelector.
     * @return The ContextSelector.
     */
    public ContextSelector getSelector() {
        return selector;
    }

	/**
	 * Returns the ShutdownCallbackRegistry
	 *
	 * @return the ShutdownCallbackRegistry
	 * @since 2.4
	 */
	public ShutdownCallbackRegistry getShutdownCallbackRegistry() {
		return shutdownCallbackRegistry;
	}

    /**
     * Removes knowledge of a LoggerContext.
     *
     * @param context The context to remove.
     */
    @Override
    public void removeContext(final com.nydia.agent.log4j.spi.LoggerContext context) {
        if (context instanceof LoggerContext) {
            selector.removeContext((LoggerContext) context);
        }
    }

    @Override
    public Cancellable addShutdownCallback(final Runnable callback) {
        return SHUTDOWN_HOOK_ENABLED ? shutdownCallbackRegistry.addShutdownCallback(callback) : null;
    }

}
