package org.apache.hadoop.tools.posum.common.records.dataentity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.mongojack.Id;

/**
 * Created by ane on 3/4/16.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GeneralDataEntity {

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

        GeneralDataEntity that = (GeneralDataEntity) o;

        return id.equals(that.id);

    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
