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
package com.nydia.agent.log4j.core.filter;

import com.nydia.agent.log4j.Level;
import com.nydia.agent.log4j.Marker;
import com.nydia.agent.log4j.core.AbstractLifeCycle;
import com.nydia.agent.log4j.core.Filter;
import com.nydia.agent.log4j.core.LifeCycle2;
import com.nydia.agent.log4j.core.LogEvent;
import com.nydia.agent.log4j.core.Logger;
import com.nydia.agent.log4j.core.config.Node;
import com.nydia.agent.log4j.core.config.plugins.Plugin;
import com.nydia.agent.log4j.core.config.plugins.PluginElement;
import com.nydia.agent.log4j.core.config.plugins.PluginFactory;
import com.nydia.agent.log4j.core.util.ObjectArrayIterator;
import com.nydia.agent.log4j.message.Message;
import com.nydia.agent.log4j.util.PerformanceSensitive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.nydia.agent.log4j.Level;
import com.nydia.agent.log4j.Marker;
import com.nydia.agent.log4j.Level;
import com.nydia.agent.log4j.Marker;


/**
 * Composes and invokes one or more filters.
 */
@Plugin(name = "filters", category = Node.CATEGORY, printObject = true)
@PerformanceSensitive("allocation")
public final class CompositeFilter extends AbstractLifeCycle implements Iterable<Filter>, Filter {

    private static final Filter[] EMPTY_FILTERS = new Filter[0];
    private final Filter[] filters;

    private CompositeFilter() {
        this.filters = EMPTY_FILTERS;
    }

    private CompositeFilter(final Filter[] filters) {
        this.filters = filters == null ? EMPTY_FILTERS : filters;
    }

    public CompositeFilter addFilter(final Filter filter) {
        if (filter == null) {
            // null does nothing
            return this;
        }
        if (filter instanceof CompositeFilter) {
            final int size = this.filters.length + ((CompositeFilter) filter).size();
            final Filter[] copy = Arrays.copyOf(this.filters, size);
            final int index = this.filters.length;
            for (final Filter currentFilter : ((CompositeFilter) filter).filters) {
                copy[index] = currentFilter;
            }
            return new CompositeFilter(copy);
        }
        final Filter[] copy = Arrays.copyOf(this.filters, this.filters.length + 1);
        copy[this.filters.length] = filter;
        return new CompositeFilter(copy);
    }

    public CompositeFilter removeFilter(final Filter filter) {
        if (filter == null) {
            // null does nothing
            return this;
        }
        // This is not a great implementation but simpler than copying Apache Commons
        // Lang ArrayUtils.removeElement() and associated bits (MutableInt),
        // which is OK since removing a filter should not be on the critical path.
        final List<Filter> filterList = new ArrayList<>(Arrays.asList(this.filters));
        if (filter instanceof CompositeFilter) {
            for (final Filter currentFilter : ((CompositeFilter) filter).filters) {
                filterList.remove(currentFilter);
            }
        } else {
            filterList.remove(filter);
        }
        return new CompositeFilter(filterList.toArray(new Filter[this.filters.length - 1]));
    }

    @Override
    public Iterator<Filter> iterator() {
        return new ObjectArrayIterator<>(filters);
    }

    /**
     * Gets a new list over the internal filter array.
     *
     * @return a new list over the internal filter array
     * @deprecated Use {@link #getFiltersArray()}
     */
    @Deprecated
    public List<Filter> getFilters() {
        return Arrays.asList(filters);
    }

    public Filter[] getFiltersArray() {
        return filters;
    }

    /**
     * Returns whether this composite contains any filters.
     *
     * @return whether this composite contains any filters.
     */
    public boolean isEmpty() {
        return this.filters.length == 0;
    }

    public int size() {
        return filters.length;
    }

    @Override
    public void start() {
        this.setStarting();
        for (final Filter filter : filters) {
            filter.start();
        }
        this.setStarted();
    }

    @Override
    public boolean stop(final long timeout, final TimeUnit timeUnit) {
        this.setStopping();
        for (final Filter filter : filters) {
            if (filter instanceof LifeCycle2) {
                ((LifeCycle2) filter).stop(timeout, timeUnit);
            } else {
                filter.stop();
            }
        }
        setStopped();
        return true;
    }

    /**
     * Returns the result that should be returned when the filter does not match the event.
     *
     * @return the Result that should be returned when the filter does not match the event.
     */
    @Override
    public Result getOnMismatch() {
        return Result.NEUTRAL;
    }

    /**
     * Returns the result that should be returned when the filter matches the event.
     *
     * @return the Result that should be returned when the filter matches the event.
     */
    @Override
    public Result getOnMatch() {
        return Result.NEUTRAL;
    }

    /**
     * Filter an event.
     *
     * @param logger
     *            The Logger.
     * @param level
     *            The event logging Level.
     * @param marker
     *            The Marker for the event or null.
     * @param msg
     *            String text to filter on.
     * @param params
     *            An array of parameters or null.
     * @return the Result.
     */
    @Override
    public Result filter(final Logger logger, final Level level, final Marker marker, final String msg,
                         final Object... params) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, params);
            if (result == Result.ACCEPT || result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    /**
     * Filter an event.
     *
     * @param logger
     *            The Logger.
     * @param level
     *            The event logging Level.
     * @param marker
     *            The Marker for the event or null.
     * @param msg
     *            String text to filter on.
     * @param p0 the message parameters
     * @return the Result.
     */
    @Override
    public Result filter(final Logger logger, final Level level, final Marker marker, final String msg,
            final Object p0) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0);
            if (result == Result.ACCEPT || result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    /**
     * Filter an event.
     *
     * @param logger
     *            The Logger.
     * @param level
     *            The event logging Level.
     * @param marker
     *            The Marker for the event or null.
     * @param msg
     *            String text to filter on.
     * @param p0 the message parameters
     * @param p1 the message parameters
     * @return the Result.
     */
    @Override
    public Result filter(final Logger logger, final Level level, final Marker marker, final String msg,
            final Object p0, final Object p1) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0, p1);
            if (result == Result.ACCEPT || result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    /**
     * Filter an event.
     *
     * @param logger
     *            The Logger.
     * @param level
     *            The event logging Level.
     * @param marker
     *            The Marker for the event or null.
     * @param msg
     *            String text to filter on.
     * @param p0 the message parameters
     * @param p1 the message parameters
     * @param p2 the message parameters
     * @return the Result.
     */
    @Override
    public Result filter(final Logger logger, final Level level, final Marker marker, final String msg,
            final Object p0, final Object p1, final Object p2) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0, p1, p2);
            if (result == Result.ACCEPT || result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    /**
     * Filter an event.
     *
     * @param logger
     *            The Logger.
     * @param level
     *            The event logging Level.
     * @param marker
     *            The Marker for the event or null.
     * @param msg
     *            String text to filter on.
     * @param p0 the message parameters
     * @param p1 the message parameters
     * @param p2 the message parameters
     * @param p3 the message parameters
     * @return the Result.
     */
    @Override
    public Result filter(final Logger logger, final Level level, final Marker marker, final String msg,
            final Object p0, final Object p1, final Object p2, final Object p3) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0, p1, p2, p3);
            if (result == Result.ACCEPT || result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    /**
     * Filter an event.
     *
     * @param logger
     *            The Logger.
     * @param level
     *            The event logging Level.
     * @param marker
     *            The Marker for the event or null.
     * @param msg
     *            String text to filter on.
     * @param p0 the message parameters
     * @param p1 the message parameters
     * @param p2 the message parameters
     * @param p3 the message parameters
     * @param p4 the message parameters
     * @return the Result.
     */
    @Override
    public Result filter(final Logger logger, final Level level, final Marker marker, final String msg,
            final Object p0, final Object p1, final Object p2, final Object p3,
            final Object p4) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0, p1, p2, p3, p4);
            if (result == Result.ACCEPT || result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    /**
     * Filter an event.
     *
     * @param logger
     *            The Logger.
     * @param level
     *            The event logging Level.
     * @param marker
     *            The Marker for the event or null.
     * @param msg
     *            String text to filter on.
     * @param p0 the message parameters
     * @param p1 the message parameters
     * @param p2 the message parameters
     * @param p3 the message parameters
     * @param p4 the message parameters
     * @param p5 the message parameters
     * @return the Result.
     */
    @Override
    public Result filter(final Logger logger, final Level level, final Marker marker, final String msg,
            final Object p0, final Object p1, final Object p2, final Object p3,
            final Object p4, final Object p5) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0, p1, p2, p3, p4, p5);
            if (result == Result.ACCEPT || result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    /**
     * Filter an event.
     *
     * @param logger
     *            The Logger.
     * @param level
     *            The event logging Level.
     * @param marker
     *            The Marker for the event or null.
     * @param msg
     *            String text to filter on.
     * @param p0 the message parameters
     * @param p1 the message parameters
     * @param p2 the message parameters
     * @param p3 the message parameters
     * @param p4 the message parameters
     * @param p5 the message parameters
     * @param p6 the message parameters
     * @return the Result.
     */
    @Override
    public Result filter(final Logger logger, final Level level, final Marker marker, final String msg,
            final Object p0, final Object p1, final Object p2, final Object p3,
            final Object p4, final Object p5, final Object p6) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0, p1, p2, p3, p4, p5, p6);
            if (result == Result.ACCEPT || result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    /**
     * Filter an event.
     *
     * @param logger
     *            The Logger.
     * @param level
     *            The event logging Level.
     * @param marker
     *            The Marker for the event or null.
     * @param msg
     *            String text to filter on.
     * @param p0 the message parameters
     * @param p1 the message parameters
     * @param p2 the message parameters
     * @param p3 the message parameters
     * @param p4 the message parameters
     * @param p5 the message parameters
     * @param p6 the message parameters
     * @param p7 the message parameters
     * @return the Result.
     */
    @Override
    public Result filter(final Logger logger, final Level level, final Marker marker, final String msg,
            final Object p0, final Object p1, final Object p2, final Object p3,
            final Object p4, final Object p5, final Object p6,
            final Object p7) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0, p1, p2, p3, p4, p5, p6, p7);
            if (result == Result.ACCEPT || result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    /**
     * Filter an event.
     *
     * @param logger
     *            The Logger.
     * @param level
     *            The event logging Level.
     * @param marker
     *            The Marker for the event or null.
     * @param msg
     *            String text to filter on.
     * @param p0 the message parameters
     * @param p1 the message parameters
     * @param p2 the message parameters
     * @param p3 the message parameters
     * @param p4 the message parameters
     * @param p5 the message parameters
     * @param p6 the message parameters
     * @param p7 the message parameters
     * @param p8 the message parameters
     * @return the Result.
     */
    @Override
    public Result filter(final Logger logger, final Level level, final Marker marker, final String msg,
            final Object p0, final Object p1, final Object p2, final Object p3,
            final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0, p1, p2, p3, p4, p5, p6, p7, p8);
            if (result == Result.ACCEPT || result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    /**
     * Filter an event.
     *
     * @param logger
     *            The Logger.
     * @param level
     *            The event logging Level.
     * @param marker
     *            The Marker for the event or null.
     * @param msg
     *            String text to filter on.
     * @param p0 the message parameters
     * @param p1 the message parameters
     * @param p2 the message parameters
     * @param p3 the message parameters
     * @param p4 the message parameters
     * @param p5 the message parameters
     * @param p6 the message parameters
     * @param p7 the message parameters
     * @param p8 the message parameters
     * @param p9 the message parameters
     * @return the Result.
     */
    @Override
    public Result filter(final Logger logger, final Level level, final Marker marker, final String msg,
            final Object p0, final Object p1, final Object p2, final Object p3,
            final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
            if (result == Result.ACCEPT || result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    /**
     * Filter an event.
     *
     * @param logger
     *            The Logger.
     * @param level
     *            The event logging Level.
     * @param marker
     *            The Marker for the event or null.
     * @param msg
     *            Any Object.
     * @param t
     *            A Throwable or null.
     * @return the Result.
     */
    @Override
    public Result filter(final Logger logger, final Level level, final Marker marker, final Object msg,
            final Throwable t) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, t);
            if (result == Result.ACCEPT || result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    /**
     * Filter an event.
     *
     * @param logger
     *            The Logger.
     * @param level
     *            The event logging Level.
     * @param marker
     *            The Marker for the event or null.
     * @param msg
     *            The Message
     * @param t
     *            A Throwable or null.
     * @return the Result.
     */
    @Override
    public Result filter(final Logger logger, final Level level, final Marker marker, final Message msg,
            final Throwable t) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, t);
            if (result == Result.ACCEPT || result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    /**
     * Filter an event.
     *
     * @param event
     *            The Event to filter on.
     * @return the Result.
     */
    @Override
    public Result filter(final LogEvent event) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(event);
            if (result == Result.ACCEPT || result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < filters.length; i++) {
            if (sb.length() == 0) {
                sb.append('{');
            } else {
                sb.append(", ");
            }
            sb.append(filters[i].toString());
        }
        if (sb.length() > 0) {
            sb.append('}');
        }
        return sb.toString();
    }

    /**
     * Create a CompositeFilter.
     *
     * @param filters
     *            An array of Filters to call.
     * @return The CompositeFilter.
     */
    @PluginFactory
    public static CompositeFilter createFilters(@PluginElement("Filters") final Filter[] filters) {
        return new CompositeFilter(filters);
    }

}
