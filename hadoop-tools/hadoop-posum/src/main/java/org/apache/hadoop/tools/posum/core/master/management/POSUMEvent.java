package org.apache.hadoop.tools.posum.core.master.management;

import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * Created by ane on 4/20/16.
 */
public class POSUMEvent extends AbstractEvent<POSUMEventType> {

    Object content;

    public POSUMEvent(POSUMEventType eventType, Object content) {
        super(eventType);
        this.content = content;
    }

    public POSUMEvent(POSUMEventType eventType) {
        super(eventType);
    }

    public <T> T getCastContent() {
        return (T) content;
    }

}
