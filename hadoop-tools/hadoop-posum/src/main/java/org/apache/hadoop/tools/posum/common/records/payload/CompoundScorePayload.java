package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.yarn.util.Records;

public abstract class CompoundScorePayload implements Payload {
  public static CompoundScorePayload newInstance(Double slowdown, Double penalty, Double cost) {
    CompoundScorePayload payload = Records.newRecord(CompoundScorePayload.class);
    payload.setSlowdown(slowdown);
    payload.setPenalty(penalty);
    payload.setCost(cost);
    return payload;
  }

  public abstract Double getSlowdown();

  public abstract void setSlowdown(Double slowdown);

  public abstract Double getPenalty();

  public abstract void setPenalty(Double penalty);

  public abstract Double getCost();

  public abstract void setCost(Double cost);

  public CompoundScorePayload subtract(CompoundScorePayload that) {
    return newInstance(
      this.getSlowdown() - that.getSlowdown(),
      this.getPenalty() - that.getPenalty(),
      this.getCost() - that.getCost()
    );
  }

  @Override
  public String toString() {
    return "CompoundScore{" +
      "runtime=" + getSlowdown() +
      "penalty=" + getPenalty() +
      "cost=" + getCost() +
      "}";
  }
}
