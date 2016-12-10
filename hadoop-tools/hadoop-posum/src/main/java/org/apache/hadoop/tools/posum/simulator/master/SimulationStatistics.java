package org.apache.hadoop.tools.posum.simulator.master;

/**
 * Created by ane on 11/25/16.
 */
public class SimulationStatistics {
    private Long startTimeCluster;
    private Long endTimeCluster;
    private Long startTimePhysical;
    private Long endTimePhysical;

    public Long getStartTimeCluster() {
        return startTimeCluster;
    }

    public void setStartTimeCluster(Long startTimeCluster) {
        this.startTimeCluster = startTimeCluster;
    }

    public Long getEndTimeCluster() {
        return endTimeCluster;
    }

    public void setEndTimeCluster(Long endTimeCluster) {
        this.endTimeCluster = endTimeCluster;
    }

    public Long getStartTimePhysical() {
        return startTimePhysical;
    }

    public void setStartTimePhysical(Long startTimePhysical) {
        this.startTimePhysical = startTimePhysical;
    }

    public Long getEndTimePhysical() {
        return endTimePhysical;
    }

    public void setEndTimePhysical(Long endTimePhysical) {
        this.endTimePhysical = endTimePhysical;
    }
}
