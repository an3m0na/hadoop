package org.apache.hadoop.tools.posum.common.records.dataentity;

import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.AppProfilePBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.CountersProxyPBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.ExternalDeadlinePBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.HistoryProfilePBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.JobConfProxyPBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.JobProfilePBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.LogEntryPBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.TaskProfilePBImpl;

public enum DataEntityCollection {
  JOB("jobs", JobProfilePBImpl.class),
  JOB_HISTORY("jobs_history", JobProfilePBImpl.class),
  TASK("tasks", TaskProfilePBImpl.class),
  TASK_HISTORY("tasks_history", TaskProfilePBImpl.class),
  APP("apps", AppProfilePBImpl.class),
  APP_HISTORY("apps_history", AppProfilePBImpl.class),
  JOB_CONF("job_confs", JobConfProxyPBImpl.class),
  JOB_CONF_HISTORY("job_confs_history", JobConfProxyPBImpl.class),
  COUNTER("counters", CountersProxyPBImpl.class),
  COUNTER_HISTORY("counters_history", CountersProxyPBImpl.class),
  HISTORY("history", HistoryProfilePBImpl.class),
  POSUM_STATS("posum_stats", LogEntryPBImpl.class),
  PREDICTOR_LOG("predictor_log", LogEntryPBImpl.class),
  AUDIT_LOG("audit_log", LogEntryPBImpl.class),
  DEADLINE("deadlines", ExternalDeadlinePBImpl.class);

  private String label;
  private Class<? extends GeneralDataEntity> mappedClass;

  DataEntityCollection(String label, Class<? extends GeneralDataEntity> mappedClass) {
    this.label = label;
    this.mappedClass = mappedClass;
  }

  public Integer getId() {
    return ordinal();
  }

  public String getLabel() {
    return label;
  }

  public Class<? extends GeneralDataEntity> getMappedClass() {
    return mappedClass;
  }

  public static DataEntityCollection fromLabel(String label) {
    for (DataEntityCollection type : DataEntityCollection.values()) {
      if (type.getLabel().equals(label))
        return type;
    }
    return null;
  }

}
