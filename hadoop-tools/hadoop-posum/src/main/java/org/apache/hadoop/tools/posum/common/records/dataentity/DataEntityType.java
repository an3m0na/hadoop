package org.apache.hadoop.tools.posum.common.records.dataentity;

import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.AppProfilePBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.HistoryProfilePBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.JobProfilePBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.TaskProfilePBImpl;

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
    private Class mappedClass;

    DataEntityType(String label, Class mappedClass) {
        this.label = label;
        this.mappedClass = mappedClass;
    }

    public Integer getId() {
        return ordinal();
    }

    public String getLabel() {
        return label;
    }

    public Class getMappedClass() {
        return mappedClass;
    }

}
