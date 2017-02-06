package org.apache.hadoop.tools.posum.scheduler.portfolio.singleq;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Set;

public class SQSchedulerNode extends SchedulerNode {

    private static Log logger = LogFactory.getLog(SQSchedulerNode.class);
    private final SQSchedulerNode inner;
    private final boolean viaInner;

    public SQSchedulerNode(RMNode node, boolean usePortForNodeName) {
        super(node, usePortForNodeName);
        this.inner = this;
        this.viaInner = false;
    }

    public SQSchedulerNode(RMNode node, boolean usePortForNodeName, Set<String> labels) {
        super(node, usePortForNodeName, labels);
        this.inner = this;
        this.viaInner = false;
    }

    public SQSchedulerNode(SQSchedulerNode inner) {
        super(inner.getRMNode(), true);
        this.inner = inner;
        this.viaInner = true;
    }

    static <N extends SQSchedulerNode> N getInstance(Class<N> nClass, RMNode node, boolean usePortForNodeName) {
        try {
            Constructor<N> constructor = nClass.getConstructor(RMNode.class, boolean.class);
            return constructor.newInstance(node, usePortForNodeName);
        } catch (Exception e) {
            throw new PosumException("Failed to instantiate scheduler node via constructor" + e);
        }
    }

    static <N extends SQSchedulerNode> N getInstance(Class<N> nClass, SQSchedulerNode node) {
        try {
            Constructor<N> constructor = nClass.getConstructor(SQSchedulerNode.class);
            return constructor.newInstance(node);
        } catch (Exception e) {
            throw new PosumException("Failed to instantiate scheduler node via constructor" + e);
        }
    }

    @Override
    public synchronized void reserveResource(SchedulerApplicationAttempt applicationAttempt,
                                             Priority priority,
                                             RMContainer container) {
        if (viaInner) {
            inner.reserveResource(applicationAttempt, priority, container);
            return;
        }

        // Same for everyone

        // Check if it's already reserved
        RMContainer reservedContainer = getReservedContainer();
        if (reservedContainer != null) {
            // Sanity check
            if (!container.getContainer().getNodeId().equals(getNodeID())) {
                throw new IllegalStateException("Trying to reserve" +
                        " container " + container +
                        " on node " + container.getReservedNode() +
                        " when currently" + " reserved resource " + reservedContainer +
                        " on node " + reservedContainer.getReservedNode());
            }

            // Cannot reserve more than one application attempt on a given node!
            // Reservation is still against attempt.
            if (!reservedContainer.getContainer().getId().getApplicationAttemptId()
                    .equals(container.getContainer().getId().getApplicationAttemptId())) {
                throw new IllegalStateException("Trying to reserve" +
                        " container " + container +
                        " for application " + applicationAttempt.getApplicationAttemptId() +
                        " when currently" +
                        " reserved container " + reservedContainer +
                        " on node " + this);
            }

            logger.debug("Updated reserved container "
                    + container.getContainer().getId() + " on node " + this
                    + " for application attempt "
                    + applicationAttempt.getApplicationAttemptId());
        } else {
            logger.debug("Reserved container "
                    + container.getContainer().getId() + " on node " + this
                    + " for application attempt "
                    + applicationAttempt.getApplicationAttemptId());
        }
        setReservedContainer(container);

    }

    @Override
    public synchronized void unreserveResource(SchedulerApplicationAttempt applicationAttempt) {

        if (viaInner) {
            inner.unreserveResource(applicationAttempt);
            return;
        }

        // Same for everyone (checks only on FIFO)

        // adding NP checks as this can now be called for preemption
        if (getReservedContainer() != null
                && getReservedContainer().getContainer() != null
                && getReservedContainer().getContainer().getId() != null
                && getReservedContainer().getContainer().getId()
                .getApplicationAttemptId() != null) {

            // Cannot unreserve for wrong application...
            ApplicationAttemptId reservedApplication =
                    getReservedContainer().getContainer().getId()
                            .getApplicationAttemptId();
            if (!reservedApplication.equals(
                    applicationAttempt.getApplicationAttemptId())) {
                throw new IllegalStateException("Trying to unreserve " +
                        " for application " + applicationAttempt.getApplicationAttemptId() +
                        " when currently reserved " +
                        " for application " + reservedApplication.getApplicationId() +
                        " on node " + this);
            }
        }
        setReservedContainer(null);
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
            inner.setReservedContainer(reservedContainer);
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
