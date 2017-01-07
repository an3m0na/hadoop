package org.apache.hadoop.tools.posum.common.records.dataentity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
@org.codehaus.jackson.annotate.JsonIgnoreProperties(ignoreUnknown = true)
public interface GeneralDataEntity<T extends GeneralDataEntity>{

    String getId();

    void setId(String id);

    Long getLastUpdated();

    void setLastUpdated(Long timestamp);

    T copy();

}
