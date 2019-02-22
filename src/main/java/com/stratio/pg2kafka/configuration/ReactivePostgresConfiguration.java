package com.stratio.pg2kafka.configuration;

import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.reactiverse.pgclient.PgPoolOptions;
import io.reactiverse.reactivex.pgclient.PgClient;
import io.reactiverse.reactivex.pgclient.PgPool;

@Configuration
public class ReactivePostgresConfiguration {

    @Bean
    public PgPoolOptions postgresClientPoolOptions(DataSourceProperties dataSourceProperties) {
        return PgPoolOptions
                .fromUri(StringUtils.substringAfter(dataSourceProperties.determineUrl(), "jdbc:"))
                .setUser(dataSourceProperties.getUsername())
                .setPassword(dataSourceProperties.getPassword());
    }

    @Bean
    public PgPool postgresClientPool(PgPoolOptions pgPoolOptions) {
        return PgClient.pool(pgPoolOptions);
    }

}
