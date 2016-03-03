package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.tools.posum.common.records.AppProfile;
import org.apache.hadoop.tools.posum.common.records.JobProfile;
import org.apache.hadoop.tools.posum.common.records.TaskProfile;

/**
 * Created by ane on 3/3/16.
 */
public enum DataCollection {
    JOBS("jobs", JobProfile.class),
    TASKS("tasks", TaskProfile.class),
    APPS("apps", AppProfile.class);

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

    static DataCollection getByClass(Class tclass) {
        for (DataCollection value : DataCollection.values()) {
            if (value.getMappedClass().equals(tclass))
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
