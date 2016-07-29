package org.apache.hadoop.tools.posum.common.records.field;

import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 4/20/16.
 */
public abstract class CompoundScore {
    public static CompoundScore newInstance(Double runtime, Double penalty, Double cost) {
        CompoundScore payload = Records.newRecord(CompoundScore.class);
        payload.setRuntime(runtime);
        payload.setPenalty(penalty);
        payload.setCost(cost);
        return payload;
    }

    public abstract Double getRuntime();

    public abstract void setRuntime(Double runtime);

    public abstract Double getPenalty();

    public abstract void setPenalty(Double penalty);

    public abstract Double getCost();

    public abstract void setCost(Double cost);

    public Double calculateValue() {
        //TODO change to include all
        Double runtime = getRuntime();
        if (runtime != null)
            return runtime;
        return 0.0;
    }

    @Override
    public String toString() {
        return "CompoundScore{" +
                "runtime=" + getRuntime() +
                "penalty=" + getPenalty() +
                "cost=" + getCost() +
                "}";
    }
}
