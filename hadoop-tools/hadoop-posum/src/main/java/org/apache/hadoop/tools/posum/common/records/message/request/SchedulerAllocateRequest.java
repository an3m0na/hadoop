package org.apache.hadoop.tools.posum.common.records.message.request;

import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * Created by ane on 4/5/16.
 */
public abstract class SchedulerAllocateRequest {

    public static SchedulerAllocateRequest newInstance(ApplicationAttemptId applicationAttemptId,
                                                       List<ResourceRequest> ask,
                                                       List<ContainerId> release,
                                                       List<String> blacklistAdditions,
                                                       List<String> blacklistRemovals) {
        SchedulerAllocateRequest request = Records.newRecord(SchedulerAllocateRequest.class);
        request.setApplicationAttemptId(applicationAttemptId);
        request.setResourceRequests(ask);
        request.setReleases(release);
        request.setBlacklistAdditions(blacklistAdditions);
        request.setBlacklistRemovals(blacklistRemovals);
        return request;
    }

    public abstract ApplicationAttemptId getApplicationAttemptId();

    public abstract void setApplicationAttemptId(ApplicationAttemptId applicationAttemptId);

    public abstract List<ResourceRequest> getResourceRequests();

    public abstract void setResourceRequests(List<ResourceRequest> resourceRequests);

    public abstract List<ContainerId> getReleases();

    public abstract void setReleases(List<ContainerId> releases);

    public abstract List<String> getBlacklistAdditions();

    public abstract void setBlacklistAdditions(List<String> blacklistAdditions);

    public abstract List<String> getBlacklistRemovals();

    public abstract void setBlacklistRemovals(List<String> blacklistRemovals);


}

