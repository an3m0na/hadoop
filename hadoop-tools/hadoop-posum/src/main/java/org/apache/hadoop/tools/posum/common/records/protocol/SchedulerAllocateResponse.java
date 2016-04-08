package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 4/5/16.
 */
public abstract class SchedulerAllocateResponse {

    public static SchedulerAllocateResponse newInstance(Allocation allocation) {
        SchedulerAllocateResponse response = Records.newRecord(SchedulerAllocateResponse.class);
        response.setAllocation(allocation);
        return response;
    }

    public abstract Allocation getAllocation();

    public abstract void setAllocation(Allocation allocation);


}

