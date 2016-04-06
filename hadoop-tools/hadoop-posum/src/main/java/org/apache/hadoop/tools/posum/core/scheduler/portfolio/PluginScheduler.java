package org.apache.hadoop.tools.posum.core.scheduler.portfolio;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.*;

import java.io.IOException;
import java.util.List;

/**
 * Created by ane on 4/1/16.
 */
public abstract class PluginScheduler<
        A extends SchedulerApplicationAttempt,
        N extends SchedulerNode,
        S extends PluginScheduler<A, N, S>>
        extends AbstractYarnScheduler<A, N> implements Configurable {

    protected Class<A> aClass;
    protected Class<N> nClass;
    protected Configuration pluginConf;

    public PluginScheduler(Class<A> aClass, Class<N> nClass, Class<S> sClass) {
        super(sClass.getName());
        this.aClass = aClass;
        this.nClass = nClass;
    }

    protected void initializePlugin(Configuration conf) {
        this.pluginConf = conf;
    }

}
