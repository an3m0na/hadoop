package org.apache.hadoop.tools.posum.common.records.dataentity;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(using = HistoryProfileDeserializer.class)
public interface HistoryProfile<T extends GeneralDataEntity> extends GeneralDataEntity<HistoryProfile> {

     T getOriginal();

     void setOriginal(T original);

     DataEntityCollection getType();

     void setType(DataEntityCollection type);

     String getOriginalId();

     void setOriginalId(String originalId);
}
