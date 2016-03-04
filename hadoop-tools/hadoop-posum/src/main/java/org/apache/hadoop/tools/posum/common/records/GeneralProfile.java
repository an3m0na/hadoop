package org.apache.hadoop.tools.posum.common.records;

import org.codehaus.jackson.annotate.JsonProperty;
import org.mongojack.Id;

/**
 * Created by ane on 3/4/16.
 */
public class GeneralProfile {

    @Id
    @JsonProperty("_id")
    String id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "id='" + id + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AppProfile that = (AppProfile) o;

        return id.equals(that.id);

    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
