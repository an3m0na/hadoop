package org.apache.hadoop.tools.posum.common.records.field;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * Needed because createContainerReport and setAMContainer are called on the RMContainer obtained from the scheduler
 * TODO
 */

public class POSUMContainerImpl extends RMContainerImpl {
    public POSUMContainerImpl(Container container, ApplicationAttemptId appAttemptId, NodeId nodeId, String user, RMContext rmContext) {
        super(container, appAttemptId, nodeId, user, rmContext);
    }

    @Override
    public RMContainerState getState() {
        throw new NotImplementedException();
    }

    @Override
    public Resource getReservedResource() {
        throw new NotImplementedException();
    }

    @Override
    public NodeId getReservedNode() {
        throw new NotImplementedException();
    }

    @Override
    public Priority getReservedPriority() {
        throw new NotImplementedException();
    }

    @Override
    public void handle(RMContainerEvent event) {
        throw new NotImplementedException();
    }

    @Override
    public void setAMContainer(boolean isAMContainer) {
        super.setAMContainer(isAMContainer);
        //TODO send this info back to scheduler
    }
}
