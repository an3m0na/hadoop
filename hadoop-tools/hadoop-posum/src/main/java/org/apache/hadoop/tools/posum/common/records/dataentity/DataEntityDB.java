package org.apache.hadoop.tools.posum.common.records.dataentity;

import org.apache.hadoop.yarn.util.Records;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.tools.posum.common.util.Utils.safeEquals;
import static org.apache.hadoop.tools.posum.common.util.Utils.safeHashCode;

/**
 * Created by ane on 5/17/16.
 */

public abstract class DataEntityDB {
    private static final String ROOT = "posum";
    private static final String SEPARATOR = "_";

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

    public static DataEntityDB get(Type type) {
        if (type == null)
            return null;
        DataEntityDB db = Records.newRecord(DataEntityDB.class);
        db.setType(type);
        return db;
    }

    public static DataEntityDB get(Type type, String view) {
        DataEntityDB db = get(type);
        if (view != null && view.length() > 0)
            db.setView(view);
        return db;
    }

    protected abstract void setType(Type type);

    protected abstract void setView(String view);

    protected abstract Type getType();

    protected abstract String getView();


    public String getName() {
        String view = getView();
        return ROOT + SEPARATOR + getType().getLabel() + (view != null ? SEPARATOR + view : "");
    }

    public static DataEntityDB fromName(String name) {
        if (name == null || !name.startsWith(ROOT + SEPARATOR))
            return null;
        String dbLabel = name.substring(ROOT.length() + 1);
        String view = "";
        int separatorIndex = dbLabel.indexOf(SEPARATOR);
        if (separatorIndex > 0) {
            view = dbLabel.substring(separatorIndex + 1);
            dbLabel = dbLabel.substring(0, separatorIndex);
        }
        DataEntityDB db = Records.newRecord(DataEntityDB.class);
        for (Type type : Type.values()) {
            if (dbLabel.equals(type.getLabel())) {
                db.setType(type);
                if (view.length() > 0)
                    db.setView(view);
                return db;
            }
        }
        return null;
    }

    public static DataEntityDB getMain() {
        return get(Type.MAIN);
    }

    public static DataEntityDB getLogs() {
        return get(Type.LOGS);
    }

    public static DataEntityDB getSimulation() {
        return get(Type.SIMULATION);
    }

    public Integer getId() {
        return getType().ordinal();
    }

    public boolean isView() {
        return getView() != null;
    }

    public static List<DataEntityDB> listByType() {
        Type[] types = Type.values();
        List<DataEntityDB> ret = new ArrayList<>(types.length);
        for (Type type : types) {
            ret.add(get(type));
        }
        return ret;
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


