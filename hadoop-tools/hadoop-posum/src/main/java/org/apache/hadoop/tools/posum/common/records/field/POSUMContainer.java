package org.apache.hadoop.tools.posum.common.records.field;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * TODO This needs to work:
 * <p>
 * containerReport = ContainerReport.newInstance(this.getContainerId(),
 * this.getAllocatedResource(), this.getAllocatedNode(),
 * this.getAllocatedPriority(), this.getCreationTime(),
 * this.getFinishTime(), this.getDiagnosticsInfo(), this.getLogURL(),
 * this.getContainerExitStatus(), this.getContainerState(),
 * this.getNodeHttpAddress());
 */
public  abstract class POSUMContainer implements RMContainer {
//    public POSUMContainer(Container container, ApplicationAttemptId appAttemptId, NodeId nodeId, String user, RMContext rmContext) {
//        super(container, appAttemptId, nodeId, user, rmContext);
//    }
//
//    public POSUMContainer(Container container, ApplicationAttemptId appAttemptId, NodeId nodeId, String user, RMContext rmContext, long creationTime) {
//        super(container, appAttemptId, nodeId, user, rmContext, creationTime);
//    }

    public static POSUMContainer newInstance(RMContainer original) {
        POSUMContainer rmContainer = Records.newRecord(POSUMContainer.class);

        return rmContainer;
        /**
         * these are used:
         *
         * containerReport = ContainerReport.newInstance(this.getContainerId(),
         this.getAllocatedResource(), this.getAllocatedNode(),
         this.getAllocatedPriority(), this.getCreationTime(),
         this.getFinishTime(), this.getDiagnosticsInfo(), this.getLogURL(),
         this.getContainerExitStatus(), this.getContainerState(),
         this.getNodeHttpAddress());
         */
    }

    public abstract ContainerId getContainerId();

    public abstract void setContainerId(ContainerId containerId);

    public abstract ApplicationAttemptId getApplicationAttemptId();

    public abstract void setApplicationAttemptId(ApplicationAttemptId applicationAttemptId);

    public abstract Container getContainer();

    public abstract void setContainer(Container container);

    public abstract long getCreationTime();

    public abstract long getFinishTime();

    public abstract String getDiagnosticsInfo();

    public abstract String getLogURL();

    public abstract int getContainerExitStatus();

    public abstract ContainerState getContainerState();

    public abstract ContainerReport createContainerReport();

    public abstract boolean isAMContainer();

    public abstract List<ResourceRequest> getResourceRequests();

    public abstract String getNodeHttpAddress();


}
