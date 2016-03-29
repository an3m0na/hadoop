package org.apache.hadoop.tools.posum.common.records.dataentity;

import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.*;

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
    HISTORY("history", HistoryProfilePBImpl.class);

    private String label;
    private Class<? extends GeneralDataEntityPBImpl> mappedClass;

    DataEntityType(String label, Class<? extends GeneralDataEntityPBImpl> mappedClass) {
        this.label = label;
        this.mappedClass = mappedClass;
    }

    public Integer getId() {
        return ordinal();
    }

    public String getLabel() {
        return label;
    }

    public Class<? extends GeneralDataEntityPBImpl> getMappedClass() {
        return mappedClass;
    }

}
