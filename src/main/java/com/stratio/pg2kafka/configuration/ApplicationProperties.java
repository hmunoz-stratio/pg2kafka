package com.stratio.pg2kafka.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "application")
public class ApplicationProperties {

    private EventsSource eventsSource;

    public EventsSource getEventsSource() {
        return eventsSource;
    }

    public void setEventsSource(EventsSource eventsSource) {
        this.eventsSource = eventsSource;
    }

    public static class EventsSource {

        private String channel;
        private String tableName;

        public String getChannel() {
            return channel;
        }

        public void setChannel(String channel) {
            this.channel = channel;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

    }
}
