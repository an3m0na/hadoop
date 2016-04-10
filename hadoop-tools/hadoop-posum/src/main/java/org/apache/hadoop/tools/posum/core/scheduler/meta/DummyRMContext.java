package org.apache.hadoop.tools.posum.core.scheduler.meta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.core.scheduler.meta.client.MetaSchedulerClient;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;

/**
 * Created by ane on 4/5/16.
 */
public class DummyRMContext extends RMContextImpl {
    private Dispatcher rmDispatcher;

    private class GeneralEventHandler implements EventHandler<Event> {
        private MetaSchedulerClient metaClient;

        GeneralEventHandler(Configuration posumConf) {
            metaClient = new MetaSchedulerClient();
            metaClient.init(posumConf);
            metaClient.start();
        }

        @Override
        public void handle(Event event) {
            metaClient.handleRMEvent(event);
        }
    }

    public DummyRMContext(Configuration posumConf, Configuration yarnConf) {
        setYarnConfiguration(yarnConf);
        rmDispatcher = new AsyncDispatcher();
        GeneralEventHandler handler = new GeneralEventHandler(posumConf);
        rmDispatcher.register(RMAppEventType.class, handler);
        rmDispatcher.register(RMNodeEventType.class, handler);
    }

    @Override
    public Dispatcher getDispatcher() {
        return rmDispatcher;
    }
}
