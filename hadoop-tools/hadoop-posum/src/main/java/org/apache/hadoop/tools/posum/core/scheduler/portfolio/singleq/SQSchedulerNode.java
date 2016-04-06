package org.apache.hadoop.tools.posum.core.scheduler.portfolio.singleq;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import java.lang.reflect.Constructor;

/**
 * Created by ane on 1/22/16.
 */
public class SQSchedulerNode extends SchedulerNode {

    private static Log logger = LogFactory.getLog(SQSchedulerNode.class);

    public SQSchedulerNode(RMNode node, boolean usePortForNodeName) {
        super(node, usePortForNodeName);
    }

    static <N extends SQSchedulerNode> N getInstance(Class<N> nClass, RMNode node, boolean usePortForNodeName) {
        try {
            Constructor<N> constructor = nClass.getConstructor(RMNode.class, boolean.class);
            return constructor.newInstance(node, usePortForNodeName);
        } catch (Exception e) {
            throw new POSUMException("Failed to instantiate scheduler node via default constructor" + e);
        }
    }

    @Override
    public synchronized void reserveResource(SchedulerApplicationAttempt applicationAttempt,
                                             Priority priority,
                                             RMContainer container) {
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
}
