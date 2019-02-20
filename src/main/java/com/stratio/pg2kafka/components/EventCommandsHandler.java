package com.stratio.pg2kafka.components;

import org.springframework.integration.context.IntegrationObjectSupport;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.Assert;

public class EventCommandsHandler extends IntegrationObjectSupport {

    private final JdbcTemplate jdbcTemplate;
    private final String tableName;

    public EventCommandsHandler(JdbcTemplate jdbcTemplate, String tableName) {
        Assert.notNull(jdbcTemplate, "'jdbcTemplate' must not be null");
        Assert.hasText(tableName, "'tableName' must not be empty");
        this.jdbcTemplate = jdbcTemplate;
        this.tableName = tableName;
    }

    @Override
    public String getComponentType() {
        return "eventCommands:handler";
    }

    public void tryLock(long eventId) {
        jdbcTemplate.queryForObject(
                "SELECT id FROM " + tableName + " WHERE id = ? AND process_date IS NULL FOR UPDATE NOWAIT",
                Long.class, eventId);
    }

    public void markProcessed(long eventId) {
        jdbcTemplate.update("UPDATE " + tableName + " SET process_date = now() WHERE id = ? AND process_date IS NULL",
                eventId);
    }

}
