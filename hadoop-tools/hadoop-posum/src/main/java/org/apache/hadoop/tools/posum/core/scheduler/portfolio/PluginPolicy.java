package org.apache.hadoop.tools.posum.core.scheduler.portfolio;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

/**
 * Created by ane on 4/1/16.
 */
public abstract class PluginPolicy<
        A extends SchedulerApplicationAttempt,
        N extends SchedulerNode,
        S extends PluginPolicy<A, N, S>>
        extends AbstractYarnScheduler<A, N> implements Configurable {

    protected Class<A> aClass;
    protected Class<N> nClass;
    protected Configuration pluginConf;

    public PluginPolicy(Class<A> aClass, Class<N> nClass, Class<S> sClass) {
        super(sClass.getName());
        this.aClass = aClass;
        this.nClass = nClass;
    }

    public void initializePlugin(Configuration conf) {
        this.pluginConf = conf;
    }

    public void forwardCompletedContainer(RMContainer rmContainer, ContainerStatus containerStatus, RMContainerEventType event){
        completedContainer(rmContainer, containerStatus, event);
    }

}
