package org.apache.hadoop.tools.posum.simulation.core;

import javax.annotation.Nonnull;

class SimulationEvent<T extends EventDetails> implements Comparable<SimulationEvent<T>> {

    enum Type {
        TASK_FINISHED
    }

    private Type type;
    private Long timestamp;
    private T details;

    SimulationEvent(@Nonnull Type type, @Nonnull Long timestamp, T details) {
        this(type, timestamp);
        this.details = details;
    }

    SimulationEvent(Type type, Long timestamp) {
        this.type = type;
        this.timestamp = timestamp;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public T getDetails() {
        return details;
    }

    public void setDetails(T details) {
        this.details = details;
    }

    @Override
    public int compareTo(@Nonnull SimulationEvent<T> that) {
        return this.getTimestamp().compareTo(that.getTimestamp());
    }

}
