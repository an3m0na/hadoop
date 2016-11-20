package org.apache.hadoop.tools.posum.common.records.dataentity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Created by ane on 3/4/16.
 */

@JsonIgnoreProperties(ignoreUnknown = true)
@org.codehaus.jackson.annotate.JsonIgnoreProperties(ignoreUnknown = true)
public interface GeneralDataEntity<T extends GeneralDataEntity>{

    String getId();

    void setId(String id);

    T copy();

}
