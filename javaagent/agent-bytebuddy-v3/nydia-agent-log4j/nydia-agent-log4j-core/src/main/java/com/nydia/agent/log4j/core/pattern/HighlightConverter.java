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

import com.nydia.agent.log4j.Level;
import com.nydia.agent.log4j.core.LogEvent;
import com.nydia.agent.log4j.core.config.Configuration;
import com.nydia.agent.log4j.core.config.plugins.Plugin;
import com.nydia.agent.log4j.core.layout.PatternLayout;
import com.nydia.agent.log4j.util.PerformanceSensitive;
import com.nydia.agent.log4j.util.Strings;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.nydia.agent.log4j.Level;
import com.nydia.agent.log4j.Level;

/**
 * Highlight pattern converter. Formats the result of a pattern using a color appropriate for the Level in the LogEvent.
 * <p>
 * For example:
 * </p>
 *
 * <pre>
 * %highlight{%d{ ISO8601 } [%t] %-5level: %msg%n%throwable}
 * </pre>
 * <p>
 * You can define custom colors for each Level:
 * </p>
 *
 * <pre>
 * %highlight{%d{ ISO8601 } [%t] %-5level: %msg%n%throwable}{FATAL=red, ERROR=red, WARN=yellow, INFO=green, DEBUG=cyan,
 * TRACE=black}
 * </pre>
 * <p>
 * You can use a predefined style:
 * </p>
 *
 * <pre>
 * %highlight{%d{ ISO8601 } [%t] %-5level: %msg%n%throwable}{STYLE=DEFAULT}
 * </pre>
 * <p>
 * The available predefined styles are:
 * </p>
 * <ul>
 * <li>{@code Default}</li>
 * <li>{@code Log4j} - The same as {@code Default}</li>
 * <li>{@code Logback}</li>
 * </ul>
 * <p>
 * You can use whitespace around the comma and equal sign. The names in values MUST come from the
 * {@linkplain AnsiEscape} enum, case is normalized to upper-case internally.
 * </p>
 *
 * <p>
 * To disable ANSI output unconditionally, specify an additional option <code>disableAnsi=true</code>, or to
 * disable ANSI output if no console is detected, specify option <code>noConsoleNoAnsi=true</code> e.g..
 * </p>
 * <pre>
  * %highlight{%d{ ISO8601 } [%t] %-5level: %msg%n%throwable}{STYLE=DEFAULT, noConsoleNoAnsi=true}
  * </pre>
 */
@Plugin(name = "highlight", category = PatternConverter.CATEGORY)
@ConverterKeys({ "highlight" })
@PerformanceSensitive("allocation")
public final class HighlightConverter extends LogEventPatternConverter implements AnsiConverter {

    private static final Map<Level, String> DEFAULT_STYLES = new HashMap<>();

    private static final Map<Level, String> LOGBACK_STYLES = new HashMap<>();

    private static final String STYLE_KEY = "STYLE";

    private static final String STYLE_KEY_DEFAULT = "DEFAULT";

    private static final String STYLE_KEY_LOGBACK = "LOGBACK";

    private static final Map<String, Map<Level, String>> STYLES = new HashMap<>();

    static {
        // Default styles:
        DEFAULT_STYLES.put(Level.FATAL, AnsiEscape.createSequence("BRIGHT", "RED"));
        DEFAULT_STYLES.put(Level.ERROR, AnsiEscape.createSequence("BRIGHT", "RED"));
        DEFAULT_STYLES.put(Level.WARN, AnsiEscape.createSequence("YELLOW"));
        DEFAULT_STYLES.put(Level.INFO, AnsiEscape.createSequence("GREEN"));
        DEFAULT_STYLES.put(Level.DEBUG, AnsiEscape.createSequence("CYAN"));
        DEFAULT_STYLES.put(Level.TRACE, AnsiEscape.createSequence("BLACK"));
        // Logback styles:
        LOGBACK_STYLES.put(Level.FATAL, AnsiEscape.createSequence("BLINK", "BRIGHT", "RED"));
        LOGBACK_STYLES.put(Level.ERROR, AnsiEscape.createSequence("BRIGHT", "RED"));
        LOGBACK_STYLES.put(Level.WARN, AnsiEscape.createSequence("RED"));
        LOGBACK_STYLES.put(Level.INFO, AnsiEscape.createSequence("BLUE"));
        LOGBACK_STYLES.put(Level.DEBUG, AnsiEscape.createSequence((String[]) null));
        LOGBACK_STYLES.put(Level.TRACE, AnsiEscape.createSequence((String[]) null));
        // Style map:
        STYLES.put(STYLE_KEY_DEFAULT, DEFAULT_STYLES);
        STYLES.put(STYLE_KEY_LOGBACK, LOGBACK_STYLES);
    }

    /**
     * Creates a level style map where values are ANSI escape sequences given configuration options in {@code option[1]}
     * .
     * <p>
     * The format of the option string in {@code option[1]} is:
     * </p>
     *
     * <pre>
     * Level1=Value, Level2=Value, ...
     * </pre>
     *
     * <p>
     * For example:
     * </p>
     *
     * <pre>
     * ERROR=red bold, WARN=yellow bold, INFO=green, ...
     * </pre>
     *
     * <p>
     * You can use whitespace around the comma and equal sign. The names in values MUST come from the
     * {@linkplain AnsiEscape} enum, case is normalized to upper-case internally.
     * </p>
     *
     * @param options
     *        The second slot can optionally contain the style map.
     * @return a new map
     */
    private static Map<Level, String> createLevelStyleMap(final String[] options) {
        if (options.length < 2) {
            return DEFAULT_STYLES;
        }
        // Feels like a hack. Should String[] options change to a Map<String,String>?
        final String string = options[1]
                .replaceAll(PatternParser.DISABLE_ANSI + "=(true|false)", Strings.EMPTY)
                .replaceAll(PatternParser.NO_CONSOLE_NO_ANSI + "=(true|false)", Strings.EMPTY);
        //
        final Map<String, String> styles = AnsiEscape.createMap(string, new String[] {STYLE_KEY});
        final Map<Level, String> levelStyles = new HashMap<>(DEFAULT_STYLES);
        for (final Map.Entry<String, String> entry : styles.entrySet()) {
            final String key = entry.getKey().toUpperCase(Locale.ENGLISH);
            final String value = entry.getValue();
            if (STYLE_KEY.equalsIgnoreCase(key)) {
                final Map<Level, String> enumMap = STYLES.get(value.toUpperCase(Locale.ENGLISH));
                if (enumMap == null) {
                    LOGGER.error("Unknown level style: " + value + ". Use one of " +
                        Arrays.toString(STYLES.keySet().toArray()));
                } else {
                    levelStyles.putAll(enumMap);
                }
            } else {
                final Level level = Level.toLevel(key, null);
                if (level == null) {
                    LOGGER.error("Unknown level name: {}; use one of {}", key, Arrays.toString(Level.values()));
                } else {
                    levelStyles.put(level, value);
                }
            }
        }
        return levelStyles;
    }

    /**
     * Gets an instance of the class.
     *
     * @param config The current Configuration.
     * @param options pattern options, may be null. If first element is "short", only the first line of the
     *                throwable will be formatted.
     * @return instance of class.
     */
    public static HighlightConverter newInstance(final Configuration config, final String[] options) {
        if (options.length < 1) {
            LOGGER.error("Incorrect number of options on style. Expected at least 1, received " + options.length);
            return null;
        }
        if (options[0] == null) {
            LOGGER.error("No pattern supplied on style");
            return null;
        }
        final PatternParser parser = PatternLayout.createPatternParser(config);
        final List<PatternFormatter> formatters = parser.parse(options[0]);
        final boolean disableAnsi = Arrays.toString(options).contains(PatternParser.DISABLE_ANSI + "=true");
        final boolean noConsoleNoAnsi = Arrays.toString(options).contains(PatternParser.NO_CONSOLE_NO_ANSI + "=true");
        final boolean hideAnsi = disableAnsi || (noConsoleNoAnsi && System.console() == null);
        return new HighlightConverter(formatters, createLevelStyleMap(options), hideAnsi);
    }

    private final Map<Level, String> levelStyles;

    private final List<PatternFormatter> patternFormatters;

    private final boolean noAnsi;

    private final String defaultStyle;

    /**
     * Construct the converter.
     *
     * @param patternFormatters
     *            The PatternFormatters to generate the text to manipulate.
     * @param noAnsi
     *            If true, do not output ANSI escape codes.
     */
    private HighlightConverter(final List<PatternFormatter> patternFormatters, final Map<Level, String> levelStyles, final boolean noAnsi) {
        super("style", "style");
        this.patternFormatters = patternFormatters;
        this.levelStyles = levelStyles;
        this.defaultStyle = AnsiEscape.getDefaultStyle();
        this.noAnsi = noAnsi;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void format(final LogEvent event, final StringBuilder toAppendTo) {
        int start = 0;
        int end = 0;
        if (!noAnsi) { // use ANSI: set prefix
            start = toAppendTo.length();
            toAppendTo.append(levelStyles.get(event.getLevel()));
            end = toAppendTo.length();
        }

        //noinspection ForLoopReplaceableByForEach
        for (int i = 0, size = patternFormatters.size(); i <  size; i++) {
            patternFormatters.get(i).format(event, toAppendTo);
        }

        // if we use ANSI we need to add the postfix or erase the unnecessary prefix
        final boolean empty = toAppendTo.length() == end;
        if (!noAnsi) {
            if (empty) {
                toAppendTo.setLength(start); // erase prefix
            } else {
                toAppendTo.append(defaultStyle); // add postfix
            }
        }
    }

    String getLevelStyle(Level level) {
        return levelStyles.get(level);
    }

    @Override
    public boolean handlesThrowable() {
        for (final PatternFormatter formatter : patternFormatters) {
            if (formatter .handlesThrowable()) {
                return true;
            }
        }
        return false;
    }

}
