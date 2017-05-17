package org.apache.hadoop.tools.posum.simulation.core.dispatcher;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class ApplicationEvent extends AbstractEvent<ApplicationEventType> {

  private final String oldAppId;
  private final ApplicationId newAppId;

  public ApplicationEvent(ApplicationEventType simulationEventType, String oldAppId, ApplicationId newAppId) {
    super(simulationEventType);
    this.oldAppId = oldAppId;
    this.newAppId = newAppId;
  }

  public String getOldAppId() {
    return oldAppId;
  }

  public ApplicationId getNewAppId() {
    return newAppId;
  }
}
