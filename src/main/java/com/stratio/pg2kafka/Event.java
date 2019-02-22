package com.stratio.pg2kafka;

import java.time.OffsetDateTime;
import java.util.Map;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Event {

    private String schema;
    private String table;
    private String action;
    @JsonProperty("data")
    private EventData eventData;

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public EventData getEventData() {
        return eventData;
    }

    public void setEventData(EventData eventData) {
        this.eventData = eventData;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

    public static class EventData {

        private Long id;
        @JsonProperty("data")
        private Map<String, Object> message;
        @JsonProperty("target_topic")
        private String targetTopic;
        @JsonProperty("target_key")
        private String targetKey;
        @JsonProperty("creation_date")
        private OffsetDateTime creationDate;
        @JsonProperty("process_date")
        private OffsetDateTime processDate;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public Map<String, Object> getMessage() {
            return message;
        }

        public void setMessage(Map<String, Object> message) {
            this.message = message;
        }

        public String getTargetTopic() {
            return targetTopic;
        }

        public void setTargetTopic(String targetTopic) {
            this.targetTopic = targetTopic;
        }

        public String getTargetKey() {
            return targetKey;
        }

        public void setTargetKey(String targetKey) {
            this.targetKey = targetKey;
        }

        public OffsetDateTime getCreationDate() {
            return creationDate;
        }

        public void setCreationDate(OffsetDateTime creationDate) {
            this.creationDate = creationDate;
        }

        public OffsetDateTime getProcessDate() {
            return processDate;
        }

        public void setProcessDate(OffsetDateTime processDate) {
            this.processDate = processDate;
        }

        @Override
        public String toString() {
            return ReflectionToStringBuilder.toString(this);
        }

    }
}
