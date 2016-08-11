package org.apache.hadoop.tools.posum.scheduler.portfolio.extca;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

/**
 * Created by ane on 1/22/16.
 */
public class ExtCaSchedulerNode extends FiCaSchedulerNode {

    private static Log logger = LogFactory.getLog(ExtCaSchedulerNode.class);
    private final FiCaSchedulerNode inner;
    private final boolean viaInner;

    public ExtCaSchedulerNode(RMNode node, boolean usePortForNodeName, Set<String> nodeLabels) {
        super(node, usePortForNodeName, nodeLabels);
        this.inner = this;
        this.viaInner = false;
    }

    public ExtCaSchedulerNode(ExtCaSchedulerNode oldNode) {
        super(oldNode.getRMNode(), true);
        this.inner = oldNode.inner;
        this.viaInner = true;
    }

    static <N extends ExtCaSchedulerNode> N getInstance(Class<N> nClass, RMNode node, boolean usePortForNodeName, Set<String> nodeLabels) {
        try {
            Constructor<N> constructor = nClass.getConstructor(RMNode.class, boolean.class, Set.class);
            return constructor.newInstance(node, usePortForNodeName, nodeLabels);
        } catch (Exception e) {
            throw new PosumException("Failed to instantiate scheduler node via constructor", e);
        }
    }

    static <N extends ExtCaSchedulerNode> N getInstance(Class<N> nClass, ExtCaSchedulerNode node) {
        try {
            Constructor<N> constructor = nClass.getConstructor(ExtCaSchedulerNode.class);
            return constructor.newInstance(node);
        } catch (Exception e) {
            throw new PosumException("Failed to instantiate scheduler node via constructor", e);
        }
    }

    protected void writeField(String name, Object value) {
        try {
            Field field = Utils.findField(FiCaSchedulerNode.class, name);
            field.setAccessible(true);
            field.set(inner, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new PosumException("Reflection error: ", e);
        }
    }

    protected <T> T readField(String name) {
        try {
            Field field = Utils.findField(FiCaSchedulerNode.class, name);
            field.setAccessible(true);
            return (T)field.get(inner);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new PosumException("Reflection error: ", e);
        }
    }


    protected <T> T invokeMethod(String name, Class<?>[] paramTypes, Object... args) {
        try {
            Method method = Utils.findMethod(FiCaSchedulerNode.class, name, paramTypes);
            method.setAccessible(true);
            return (T) method.invoke(inner, args);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new PosumException("Reflection error: ", e);
        }
    }

    @Override
    public void reserveResource(SchedulerApplicationAttempt application, Priority priority, RMContainer container) {
        if (viaInner) {
            inner.reserveResource(application, priority, container);
            return;
        }
        super.reserveResource(application, priority, container);
    }

    @Override
    public void unreserveResource(SchedulerApplicationAttempt application) {
        if (viaInner) {
            inner.unreserveResource(application);
            return;
        }
        super.unreserveResource(application);
    }

    @Override
    public RMNode getRMNode() {
        if (viaInner) {
            return inner.getRMNode();
        }
        return super.getRMNode();
    }

    @Override
    public synchronized void setTotalResource(Resource resource) {
        if (viaInner) {
            inner.setTotalResource(resource);
            return;
        }
        super.setTotalResource(resource);
    }

    @Override
    public NodeId getNodeID() {
        if (viaInner) {
            return inner.getNodeID();
        }
        return super.getNodeID();
    }

    @Override
    public String getHttpAddress() {
        if (viaInner) {
            return inner.getHttpAddress();
        }
        return super.getHttpAddress();
    }

    @Override
    public String getNodeName() {
        if (viaInner) {
            return inner.getNodeName();
        }
        return super.getNodeName();
    }

    @Override
    public String getRackName() {
        if (viaInner) {
            return inner.getRackName();
        }
        return super.getRackName();
    }

    @Override
    public synchronized void allocateContainer(RMContainer rmContainer) {
        if (viaInner) {
            inner.allocateContainer(rmContainer);
            return;
        }
        super.allocateContainer(rmContainer);
    }

    @Override
    public synchronized Resource getAvailableResource() {
        if (viaInner) {
            return inner.getAvailableResource();
        }
        return super.getAvailableResource();
    }

    @Override
    public synchronized Resource getUsedResource() {
        if (viaInner) {
            return inner.getUsedResource();
        }
        return super.getUsedResource();
    }

    @Override
    public synchronized Resource getTotalResource() {
        if (viaInner) {
            return inner.getTotalResource();
        }
        return super.getTotalResource();
    }

    @Override
    public synchronized boolean isValidContainer(ContainerId containerId) {
        if (viaInner) {
            return inner.isValidContainer(containerId);
        }
        return super.isValidContainer(containerId);
    }

    @Override
    public synchronized void releaseContainer(Container container) {
        if (viaInner) {
            inner.releaseContainer(container);
            return;
        }
        super.releaseContainer(container);
    }

    @Override
    public String toString() {
        if (viaInner) {
            return inner.toString();
        }
        return super.toString();
    }

    @Override
    public int getNumContainers() {
        if (viaInner) {
            return inner.getNumContainers();
        }
        return super.getNumContainers();
    }

    @Override
    public synchronized List<RMContainer> getRunningContainers() {
        if (viaInner) {
            return inner.getRunningContainers();
        }
        return super.getRunningContainers();
    }

    @Override
    public synchronized RMContainer getReservedContainer() {
        if (viaInner) {
            return inner.getReservedContainer();
        }
        return super.getReservedContainer();
    }

    @Override
    protected synchronized void setReservedContainer(RMContainer reservedContainer) {
        if (viaInner) {
            invokeMethod("setReservedContainer", new Class<?>[]{RMContainer.class}, reservedContainer);
            return;
        }
        super.setReservedContainer(reservedContainer);
    }

    @Override
    public synchronized void recoverContainer(RMContainer rmContainer) {
        if (viaInner) {
            inner.recoverContainer(rmContainer);
            return;
        }
        super.recoverContainer(rmContainer);
    }

    @Override
    public Set<String> getLabels() {
        if (viaInner) {
            return inner.getLabels();
        }
        return super.getLabels();
    }

    @Override
    public void updateLabels(Set<String> labels) {
        if (viaInner) {
            inner.updateLabels(labels);
            return;
        }
        super.updateLabels(labels);
    }
}
