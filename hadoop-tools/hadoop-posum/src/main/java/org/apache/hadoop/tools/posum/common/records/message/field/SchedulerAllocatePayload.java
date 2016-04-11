package org.apache.hadoop.tools.posum.common.records.message.field;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 4/5/16.
 */
public abstract class SchedulerAllocatePayload {

    public static SchedulerAllocatePayload newInstance(Allocation allocation) {
        SchedulerAllocatePayload response = Records.newRecord(SchedulerAllocatePayload.class);
        response.setAllocation(allocation);
        return response;
    }

    public abstract Allocation getAllocation();

    public abstract void setAllocation(Allocation allocation);


}

