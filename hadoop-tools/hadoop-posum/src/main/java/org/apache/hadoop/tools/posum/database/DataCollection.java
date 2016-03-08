package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.tools.posum.common.records.AppProfile;
import org.apache.hadoop.tools.posum.common.records.HistoryProfile;
import org.apache.hadoop.tools.posum.common.records.JobProfile;
import org.apache.hadoop.tools.posum.common.records.TaskProfile;

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

    static DataCollection getByClass(Class tClass) {
        for (DataCollection value : DataCollection.values()) {
            if (value.getMappedClass().equals(tClass))
                return value;
        }
        return null;
    }

    static DataCollection getByLabel(String label) {
        for (DataCollection value : DataCollection.values()) {
            if (value.getLabel().equals(label))
                return value;
        }
        return null;
    }

}
