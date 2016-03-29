package org.apache.hadoop.tools.posum.common.records.dataentity;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * Created by ane on 3/7/16.
 */
@JsonDeserialize(using = HistoryProfileDeserializer.class)
public interface HistoryProfile<T extends GeneralDataEntity> extends GeneralDataEntity {

     T getOriginal();

     void setOriginal(T original);

     Long getTimestamp();

     void setTimestamp(Long timestamp);

     DataEntityType getType();

     void setType(DataEntityType type);

     String getOriginalId();

     void setOriginalId(String originalId);
}
