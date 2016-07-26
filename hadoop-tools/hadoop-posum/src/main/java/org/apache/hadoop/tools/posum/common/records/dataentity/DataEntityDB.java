package org.apache.hadoop.tools.posum.common.records.dataentity;

import org.apache.hadoop.yarn.util.Records;

import static org.apache.hadoop.tools.posum.common.util.Utils.safeEquals;
import static org.apache.hadoop.tools.posum.common.util.Utils.safeHashCode;

/**
 * Created by ane on 5/17/16.
 */

public abstract class DataEntityDB {
    protected final String root = "posum";

    public enum Type {
        MAIN("main"), LOGS("logs"), SIMULATION("sim");

        private String label;

        Type(String label) {
            this.label = label;
        }

        String getLabel() {
            return label;
        }
    }


    public static DataEntityDB newInstance(Type type) {
        DataEntityDB db = Records.newRecord(DataEntityDB.class);
        db.setType(type);
        return db;
    }

    public static DataEntityDB newInstance(Type type, String view) {
        DataEntityDB db = Records.newRecord(DataEntityDB.class);
        db.setType(type);
        db.setView(view);
        return db;
    }

    protected abstract void setType(Type type);

    protected abstract void setView(String view);

    protected abstract Type getType();

    protected abstract String getView();


    public String getName() {
        String view = getView();
        return root + "_" + getType().getLabel() + (view != null ? "_" + view : "");
    }

    public static DataEntityDB getMain() {
        return newInstance(Type.MAIN);
    }

    public static DataEntityDB getLogs() {
        return newInstance(Type.LOGS);
    }

    public static DataEntityDB getSimulation() {
        return newInstance(Type.SIMULATION);
    }

    public Integer getId() {
        return getType().ordinal();
    }

    public boolean isView() {
        return getView() != null;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;
        if (other.getClass().isAssignableFrom(this.getClass())) {
            DataEntityDB that = (DataEntityDB) other;
            return safeEquals(this.getType(), that.getType()) &&
                    safeEquals(this.getView(), that.getView());
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = safeHashCode(getType());
        result = 31 * result + safeHashCode(getView());
        return result;
    }
}


