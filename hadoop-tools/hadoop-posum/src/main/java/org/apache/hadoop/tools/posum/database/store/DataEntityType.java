package org.apache.hadoop.tools.posum.database.store;

import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.HistoryProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.AppProfilePBImpl;

/**
 * Created by ane on 3/3/16.
 */
public enum DataEntityType {
    JOB("jobs", JobProfile.class),
    JOB_HISTORY("jobs_history", JobProfile.class),
    TASK("tasks", TaskProfile.class),
    TASK_HISTORY("tasks_history", TaskProfile.class),
    APP("apps", AppProfilePBImpl.class),
    APP_HISTORY("apps_history", AppProfile.class),
    HISTORY("history", HistoryProfile.class);

    private String label;
    private Class mappedClass;

    DataEntityType(String label, Class mappedClass) {
        this.label = label;
        this.mappedClass = mappedClass;
    }

    Integer getId() {
        return ordinal();
    }

    String getLabel() {
        return label;
    }

    Class getMappedClass() {
        return mappedClass;
    }

}
