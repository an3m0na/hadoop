package org.apache.hadoop.tools.posum.common.records.field;

import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 4/20/16.
 */
public abstract class SimulationResult implements Comparable<SimulationResult> {

    public static SimulationResult newInstance(String policyName, CompoundScore score) {
        SimulationResult result = Records.newRecord(SimulationResult.class);
        result.setPolicyName(policyName);
        result.setScore(score);
        return result;
    }

    public abstract String getPolicyName();

    public abstract void setPolicyName(String policyName);


    public abstract CompoundScore getScore();

    public abstract void setScore(CompoundScore score);

    @Override
    public int compareTo(SimulationResult o) {
        // if they refer to the same policy, they are considered equal
        if (this.getPolicyName().equals(o.getPolicyName()))
            return 0;
        // if there is no info one of them, it should be lower
        if (this.getScore() == null)
            return -1;
        if (o.getScore() == null)
            return 1;
        // if there is information on both, the one with a higher performance score is higher
        if (this.getScore().calculateValue() < o.getScore().calculateValue())
            return -1;
        return 1;
    }

    @Override
    public String toString() {
        return "SimulationResult{" + getPolicyName() + "=" + getScore() + "}";
    }
}
