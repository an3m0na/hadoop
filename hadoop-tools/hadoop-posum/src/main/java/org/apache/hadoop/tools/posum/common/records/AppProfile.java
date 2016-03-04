package org.apache.hadoop.tools.posum.common.records;

import org.apache.hadoop.tools.posum.common.RestClient;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.mongojack.Id;

/**
 * Created by ane on 2/24/16.
 */
public class AppProfile {

    @Id
    private String appId;
    private Long startTime;
    private Long finishTime;
    private String user;
    private String name;
    private YarnApplicationState state;
    private FinalApplicationStatus status;
    private RestClient.TrackingUI trackingUI;

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public YarnApplicationState getState() {
        return state;
    }

    public void setState(String state) {
        if (status != null)
            this.state = YarnApplicationState.valueOf(state);
    }

    public FinalApplicationStatus getStatus() {
        return status;
    }

    public void setStatus(String status) {
        if (status != null)
            this.status = FinalApplicationStatus.valueOf(status);
    }

    public Long getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(Long finishTime) {
        this.finishTime = finishTime;
    }

    public RestClient.TrackingUI getTrackingUI() {
        return trackingUI;
    }

    public void setTrackingUI(String trackingUI) {
        if (trackingUI != null)
            this.trackingUI = RestClient.TrackingUI.fromLabel(trackingUI);
    }

    @Override
    public String toString() {
        return "AppProfile{" +
                "appId=" + appId +
                ", startTime=" + startTime +
                ", finishTime=" + finishTime +
                ", user='" + user + '\'' +
                ", name='" + name + '\'' +
                ", state=" + state +
                ", status=" + status +
                ", trackingUI=" + trackingUI +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AppProfile that = (AppProfile) o;

        return appId.equals(that.appId);

    }

    @Override
    public int hashCode() {
        return appId.hashCode();
    }
}
