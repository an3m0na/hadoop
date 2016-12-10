package org.apache.hadoop.tools.posum.orchestrator.management;

import org.apache.hadoop.yarn.event.AbstractEvent;

public class PosumEvent extends AbstractEvent<PosumEventType> {

    Object content;

    public PosumEvent(PosumEventType eventType, Object content) {
        super(eventType);
        this.content = content;
    }

    public PosumEvent(PosumEventType eventType) {
        super(eventType);
    }

    public <T> T getCastContent() {
        return (T) content;
    }

}
