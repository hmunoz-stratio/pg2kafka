# pg2kafka

## Test environment

### PostgreSQL

Docker:

    docker run -it --name postgres -e POSTGRES_USER=docker -e POSTGRES_PASSWORD=docker -e POSTGRES_DB=postgres -p 5432:5432 --rm postgres:9.6

Execute:

```sql
CREATE TABLE IF NOT EXISTS test.event(
  id             bigserial NOT NULL,
  data           jsonb NOT NULL,
  target_topic   varchar(255) NOT NULL,
  target_key     varchar(255) NOT NULL,
  creation_date  timestamp with time zone NOT NULL DEFAULT now(),
  process_date   timestamp with time zone,
  CONSTRAINT event_pk PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS event_pending ON test.event(process_date) WHERE process_date IS NULL;

-- ----------------

CREATE OR REPLACE FUNCTION test.notify_event()
  RETURNS trigger AS
$BODY$

    DECLARE 
        data json;
        notification json;
    BEGIN
        -- Convert the old or new row to JSON, based on the kind of action.
        -- Action = DELETE?             -> OLD row
        -- Action = INSERT or UPDATE?   -> NEW row
        IF (TG_OP = 'DELETE') THEN
            data = row_to_json(OLD);
        ELSE
            data = row_to_json(NEW);
        END IF;
        
        -- Contruct the notification as a JSON string.
        notification = json_build_object(
                          'schema',TG_TABLE_SCHEMA,
                          'table',TG_TABLE_NAME,
                          'action', TG_OP,
                          'data', data);
        
                        
        -- Execute pg_notify(channel, notification)
        PERFORM pg_notify('events',notification::text);
        
        -- Result is ignored since this is an AFTER trigger
        RETURN NULL; 
    END;

$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

-- Trigger: test.notify_test_event

CREATE TRIGGER notify_test_event
AFTER INSERT ON test.event
    FOR EACH ROW EXECUTE PROCEDURE test.notify_event();
```

### Zookeeper

Docker:

    docker run -it --name zookeeper -p 2181:2181 -p 2888:2888 -p 3888:3888 --rm debezium/zookeeper

### Kafka

Docker:

    docker run -it --name kafka -p 9092:9092 --link zookeeper:zookeeper --rm debezium/kafka

### Kafka topic watcher

Docker:

    docker run -it --name kafka-watcher --link kafka:kafka --link zookeeper:zookeeper --rm debezium/kafka watch-topic -a -k test