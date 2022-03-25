package com.nydia.modules.common;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

/**
 * Returns environment identifier by current context.
 */
public class DataSourceRouter extends AbstractRoutingDataSource {

    @Override
    protected Object determineCurrentLookupKey() {
        return DatabaseContextHolder.getEnvironment();
    }

}
