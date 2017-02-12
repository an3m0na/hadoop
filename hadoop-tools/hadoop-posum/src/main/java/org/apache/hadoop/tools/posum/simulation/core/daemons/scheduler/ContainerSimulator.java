package org.apache.hadoop.tools.posum.simulation.core.daemons.scheduler;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.tools.posum.simulation.core.daemons.DaemonRunner;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

@Private
@Unstable
public class ContainerSimulator implements Delayed {
  // id
  private ContainerId id;
  // resource allocated
  private Resource resource;
  // end time
  private long endTime;
  // life time (ms)
  private long lifeTime;
  // host name
  private String hostname;
  // priority
  private int priority;
  // type
  private String type;

  /**
   * invoked when AM schedules containers to allocate
   */
  public ContainerSimulator(Resource resource, long lifeTime,
                            String hostname, int priority, String type) {
    this.resource = resource;
    this.lifeTime = lifeTime;
    this.hostname = hostname;
    this.priority = priority;
    this.type = type;
  }

  /**
   * invoke when NM schedules containers to run
   */
  public ContainerSimulator(ContainerId id, Resource resource, long endTime,
                            long lifeTime) {
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
    return unit.convert(endTime - DaemonRunner.getCurrentTime(), TimeUnit.MILLISECONDS);
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
}
