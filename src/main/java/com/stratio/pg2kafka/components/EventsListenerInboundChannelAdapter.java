package com.stratio.pg2kafka.components;

import org.springframework.integration.context.OrderlyShutdownCapable;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.util.Assert;

import io.reactiverse.reactivex.pgclient.pubsub.PgChannel;

public class EventsListenerInboundChannelAdapter extends MessageProducerSupport implements OrderlyShutdownCapable {

    private final PgChannel pgChannel;

    public EventsListenerInboundChannelAdapter(PgChannel pgChannel) {
        Assert.notNull(pgChannel, "pgChannel must not be null");
        this.pgChannel = pgChannel;
    }

    @Override
    public String getComponentType() {
        return "eventslistener:inbound-channel-adapter";
    }

    @Override
    protected void onInit() {
        pgChannel.handler(p -> sendMessage(MessageBuilder.withPayload(p).build()));
        super.onInit();
    }

    @Override
    public int beforeShutdown() {
        pgChannel.pause();
        return 0;
    }

    @Override
    public int afterShutdown() {
        return 0;
    }

}
