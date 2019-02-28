package com.stratio.pg2kafka.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Configuration
@ConfigurationProperties(prefix = "pg2kafka")
@Getter
@Setter
@ToString
public class Pg2KafkaProperties {

    private EventsSource eventsSource;

    @Getter
    @Setter
    @ToString
    public static class EventsSource {

        private String url;
        private String username;
        private String password;
        private String channel;
        private String tableName;


    }
}
