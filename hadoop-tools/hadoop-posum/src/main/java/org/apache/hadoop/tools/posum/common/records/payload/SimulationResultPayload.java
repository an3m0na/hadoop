package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.yarn.util.Records;

public abstract class SimulationResultPayload implements Payload {

  public static SimulationResultPayload newInstance(String policyName, CompoundScorePayload score) {
    SimulationResultPayload result = Records.newRecord(SimulationResultPayload.class);
    result.setPolicyName(policyName);
    result.setScore(score);
    return result;
  }

  public abstract String getPolicyName();

  public abstract void setPolicyName(String policyName);


  public abstract CompoundScorePayload getScore();

  public abstract void setScore(CompoundScorePayload score);

  @Override
  public String toString() {
    return "SimulationResult{" + getPolicyName() + "=" + getScore() + "}";
  }
}
