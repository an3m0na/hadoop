package org.apache.hadoop.tools.posum.common.records.dataentity;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.hadoop.tools.posum.common.records.HistoryProfileDeserializer;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.GeneralDataEntityPBImpl;

/**
 * Created by ane on 3/7/16.
 */
@JsonDeserialize(using = HistoryProfileDeserializer.class)
public interface HistoryProfile<T extends GeneralDataEntityPBImpl> extends GeneralDataEntity {

     T getOriginal();

     void setOriginal(T original);

     Long getTimestamp();

     void setTimestamp(Long timestamp);

     DataEntityType getType();

     void setType(DataEntityType type);

     String getOriginalId();

     void setOriginalId(String originalId);
}
