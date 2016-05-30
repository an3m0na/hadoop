package org.apache.hadoop.tools.posum.common.records.dataentity;

import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.*;
import org.apache.hadoop.tools.posum.common.records.field.impl.pb.StringStringMapPayloadPBImpl;

/**
 * Created by ane on 3/3/16.
 */
public enum DataEntityType {
    JOB("jobs", JobProfilePBImpl.class),
    JOB_HISTORY("jobs_history", JobProfilePBImpl.class),
    TASK("tasks", TaskProfilePBImpl.class),
    TASK_HISTORY("tasks_history", TaskProfilePBImpl.class),
    APP("apps", AppProfilePBImpl.class),
    APP_HISTORY("apps_history", AppProfilePBImpl.class),
    HISTORY("history", HistoryProfilePBImpl.class),
    LOG_SCHEDULER("scheduler_log", LogEntry.class),
    JOB_CONF("job_confs", JobConfProxyPBImpl.class),
    POSUM_STATS("posum_stats", LogEntry.class);

    private String label;
    private Class<? extends GeneralDataEntity> mappedClass;

    DataEntityType(String label, Class<? extends GeneralDataEntity> mappedClass) {
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

}
