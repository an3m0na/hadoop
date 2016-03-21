package org.apache.hadoop.tools.posum.common.records.profile;

import org.apache.hadoop.tools.posum.common.RestClient;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

/**
 * Created by ane on 2/24/16.
 */
public class AppProfile extends GeneralProfile {

    private Long startTime = 0L;
    private Long finishTime = 0L;
    private String user;
    private String name;
    private YarnApplicationState state;
    private FinalApplicationStatus status;
    private RestClient.TrackingUI trackingUI;

    public AppProfile() {
    }

    public AppProfile(String id) {
        this.id = id;
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
        return "AppProfile[" + getId() + "]{" +
                "startTime=" + startTime +
                ", finishTime=" + finishTime +
                ", user='" + user + '\'' +
                ", name='" + name + '\'' +
                ", state=" + state +
                ", status=" + status +
                ", trackingUI=" + trackingUI +
                '}';
    }
}
