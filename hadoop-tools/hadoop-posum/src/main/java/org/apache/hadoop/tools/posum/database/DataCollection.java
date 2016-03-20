package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.tools.posum.common.records.profile.AppProfile;
import org.apache.hadoop.tools.posum.common.records.profile.HistoryProfile;
import org.apache.hadoop.tools.posum.common.records.profile.JobProfile;
import org.apache.hadoop.tools.posum.common.records.profile.TaskProfile;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ane on 3/3/16.
 */
public enum DataCollection {
    JOBS("jobs", JobProfile.class),
    JOBS_HISTORY("jobs_history", JobProfile.class),
    TASKS("tasks", TaskProfile.class),
    TASKS_HISTORY("tasks_history", TaskProfile.class),
    APPS("apps", AppProfile.class),
    APPS_HISTORY("apps_history", AppProfile.class),
    HISTORY("history",HistoryProfile.class);

    private String label;
    private Class mappedClass;
    DataCollection(String label, Class mappedClass) {
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
