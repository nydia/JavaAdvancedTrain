package com.mengxiu.lottery.core.plugin.enums;

import java.util.Arrays;
import java.util.Objects;

/**
 * MatchModeEnum.
 */
public enum MatchModeEnum {

    /**
     * And match mode enum.
     */
    AND(0, "and"),

    /**
     * Or match mode enum.
     */
    OR(1, "or");

    private final int code;

    private final String name;

    /**
     * all args constructor.
     *
     * @param code code
     * @param name name
     */
    MatchModeEnum(final int code, final String name) {
        this.code = code;
        this.name = name;
    }

    /**
     * get code.
     *
     * @return code
     */
    public int getCode() {
        return code;
    }

    /**
     * get name.
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Judgment code and enum eq.
     *
     * @param code          code.
     * @param matchModeEnum enum
     * @return true or false.
     */
    public static boolean match(final Integer code, final MatchModeEnum matchModeEnum) {
        return Objects.equals(matchModeEnum.getCode(), code);
    }

    /**
     * get match mode name by code.
     *
     * @param code match mode code.
     * @return match mode name.
     */
    public static String getMatchModeByCode(final int code) {
        return Arrays.stream(MatchModeEnum.values())
                .filter(e -> e.code == code).findFirst()
                .orElse(MatchModeEnum.AND)
                .getName();
    }
}
