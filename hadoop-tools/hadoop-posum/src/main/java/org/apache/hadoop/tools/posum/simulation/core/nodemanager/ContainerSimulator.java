package org.apache.hadoop.tools.posum.simulation.core.nodemanager;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.tools.posum.simulation.core.daemon.DaemonRunner;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

@Private
@Unstable
public class ContainerSimulator implements Delayed {
  // id
  private ContainerId id;
  private DaemonRunner runner;
  // resource allocated
  private Resource resource;
  // end time
  private long endTime;
  // life time (ms)
  private long lifeTime;
  // host name
  private String hostname;
  // rack
  private String rack;
  // priority
  private int priority;
  // type
  private String type;

  /**
   * invoked when AM schedules containers to allocate
   */
  public ContainerSimulator(DaemonRunner runner, Resource resource, long lifeTime,
                            String rack, String hostname, int priority, String type) {
    this.runner = runner;
    this.resource = resource;
    this.lifeTime = lifeTime;
    this.hostname = hostname;
    this.priority = priority;
    this.type = type;
    this.rack = rack;
  }

  /**
   * invoke when NM schedules containers to run
   */
  public ContainerSimulator(DaemonRunner runner, ContainerId id, Resource resource, long endTime,
                            long lifeTime) {
    this.runner = runner;
    this.id = id;
    this.resource = resource;
    this.endTime = endTime;
    this.lifeTime = lifeTime;
  }

  public Resource getResource() {
    return resource;
  }

  public ContainerId getId() {
    return id;
  }

  @Override
  public int compareTo(Delayed o) {
    if (!(o instanceof ContainerSimulator)) {
      throw new IllegalArgumentException(
        "Parameter must be a ContainerSimulator instance");
    }
    ContainerSimulator other = (ContainerSimulator) o;
    return (int) Math.signum(endTime - other.endTime);
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(endTime - runner.getCurrentTime(), TimeUnit.MILLISECONDS);
  }

  public long getLifeTime() {
    return lifeTime;
  }

  public String getHostname() {
    return hostname;
  }

  public long getEndTime() {
    return endTime;
  }

  public int getPriority() {
    return priority;
  }

  public String getType() {
    return type;
  }

  public void setPriority(int p) {
    priority = p;
  }

  public String getRack() {
    return rack;
  }

  public void setRack(String rack) {
    this.rack = rack;
  }
}
