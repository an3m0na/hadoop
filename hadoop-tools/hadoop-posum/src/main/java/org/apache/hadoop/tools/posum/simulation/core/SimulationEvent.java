package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.hadoop.yarn.event.AbstractEvent;

import javax.annotation.Nonnull;

class SimulationEvent<T extends EventDetails> extends AbstractEvent<SimulationEventType> implements Comparable<SimulationEvent<T>> {

  private T details;

  public SimulationEvent(@Nonnull SimulationEventType type, @Nonnull Long timestamp, T details) {
    this(type, timestamp);
    this.details = details;
  }

  public SimulationEvent(SimulationEventType type, Long timestamp) {
    super(type, timestamp);
  }

  public T getDetails() {
    return details;
  }

  public void setDetails(T details) {
    this.details = details;
  }

  @Override
  public int compareTo(@Nonnull SimulationEvent<T> that) {
    return Float.valueOf(Math.signum(this.getTimestamp() - that.getTimestamp())).intValue();
  }

}
