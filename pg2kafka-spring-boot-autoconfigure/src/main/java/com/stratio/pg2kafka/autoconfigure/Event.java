package com.stratio.pg2kafka.autoconfigure;

import java.time.OffsetDateTime;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Event {

    private String schema;
    private String table;
    private String action;
    @JsonProperty("data")
    private EventData eventData;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
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

    }
}
