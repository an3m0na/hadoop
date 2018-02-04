package org.apache.hadoop.tools.posum.common.records.dataentity;

import org.apache.hadoop.yarn.util.Records;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.tools.posum.common.util.GeneralUtils.safeEquals;
import static org.apache.hadoop.tools.posum.common.util.GeneralUtils.safeHashCode;


public abstract class DatabaseReference {
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

  public static DatabaseReference get(Type type) {
    if (type == null)
      return null;
    DatabaseReference db = Records.newRecord(DatabaseReference.class);
    db.setType(type);
    return db;
  }

  public static DatabaseReference get(Type type, String view) {
    DatabaseReference db = get(type);
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

  public static DatabaseReference fromName(String name) {
    if (name == null || !name.startsWith(ROOT + SEPARATOR))
      return null;
    String dbLabel = name.substring(ROOT.length() + 1);
    String view = "";
    int separatorIndex = dbLabel.indexOf(SEPARATOR);
    if (separatorIndex > 0) {
      view = dbLabel.substring(separatorIndex + 1);
      dbLabel = dbLabel.substring(0, separatorIndex);
    }
    DatabaseReference db = Records.newRecord(DatabaseReference.class);
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

  public static DatabaseReference getMain() {
    return get(Type.MAIN);
  }

  public static DatabaseReference getLogs() {
    return get(Type.LOGS);
  }

  public static DatabaseReference getSimulation() {
    return get(Type.SIMULATION);
  }

  public boolean isOfType(Type type) {
    return getType().equals(type);
  }

  public boolean isView() {
    return getView() != null;
  }

  public static List<DatabaseReference> listByType() {
    Type[] types = Type.values();
    List<DatabaseReference> ret = new ArrayList<>(types.length);
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
      DatabaseReference that = (DatabaseReference) other;
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


