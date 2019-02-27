package com.stratio.pg2kafka.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.reactiverse.pgclient.PgPoolOptions;
import io.reactiverse.reactivex.pgclient.PgClient;
import io.reactiverse.reactivex.pgclient.PgPool;
import io.vertx.core.VertxOptions;
import io.vertx.reactivex.core.Vertx;

@Configuration
public class ReactivePostgresConfiguration {

    @Bean
    public PgPoolOptions postgresClientPoolOptions(Pg2KafkaProperties pg2KafkaProperties) {
        return PgPoolOptions
                .fromUri(pg2KafkaProperties.getEventsSource().getUrl())
                .setUser(pg2KafkaProperties.getEventsSource().getUsername())
                .setPassword(pg2KafkaProperties.getEventsSource().getPassword());
    }

    @Bean
    public VertxOptions vertxOptions() {
        VertxOptions options = new VertxOptions();
        options.setMaxEventLoopExecuteTime(10L * 1000 * 1000000); // 10 seconds
        return options;
    }

    @Bean
    public PgPool postgresClientPool(VertxOptions vertxOptions, PgPoolOptions pgPoolOptions) {
        VertxOptions options = new VertxOptions();
        options.setMaxEventLoopExecuteTime(10L * 1000 * 1000000); // 10 seconds
        return PgClient.pool(Vertx.vertx(vertxOptions), pgPoolOptions);
    }


}
