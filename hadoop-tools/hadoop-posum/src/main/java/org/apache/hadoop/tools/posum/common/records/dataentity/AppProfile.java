package org.apache.hadoop.tools.posum.common.records.dataentity;

import org.apache.hadoop.tools.posum.common.util.RestClient;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

/**
 * Created by ane on 2/24/16.
 */
 public interface AppProfile extends GeneralDataEntity {

     Long getStartTime();

     void setStartTime(Long startTime);

     String getUser();

     void setUser(String user);
     String getName();

     void setName(String name);

     YarnApplicationState getState();

     void setState(YarnApplicationState state);

     FinalApplicationStatus getStatus();

     void setStatus(FinalApplicationStatus status);

     Long getFinishTime();

     void setFinishTime(Long finishTime);

     RestClient.TrackingUI getTrackingUI();

     void setTrackingUI(RestClient.TrackingUI trackingUI);
}
