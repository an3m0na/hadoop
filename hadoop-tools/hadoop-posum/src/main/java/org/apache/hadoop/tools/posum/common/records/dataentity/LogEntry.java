package org.apache.hadoop.tools.posum.common.records.dataentity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.payload.PayloadType;
@JsonDeserialize(using = LogEntryDeserializer.class)
public interface LogEntry<T extends Payload> extends GeneralDataEntity<LogEntry<T>> {

    enum Type {
        POLICY_CHANGE(PayloadType.SIMPLE_PROPERTY, DataEntityCollection.AUDIT_LOG),
        POLICY_MAP(PayloadType.POLICY_INFO_MAP, DataEntityCollection.POSUM_STATS),
        TASK_PREDICTION(PayloadType.TASK_PREDICTION, DataEntityCollection.PREDICTOR_LOG),
        GENERAL(PayloadType.SIMPLE_PROPERTY, DataEntityCollection.AUDIT_LOG);

        @JsonIgnore
        private DataEntityCollection collection;
        @JsonIgnore
        private PayloadType detailsType;

        Type(PayloadType detailsType, DataEntityCollection collection) {
            this.detailsType = detailsType;
            this.collection = collection;
        }

        public DataEntityCollection getCollection() {
            return collection;
        }

        public PayloadType getDetailsType() {
            return detailsType;
        }
    }

    Type getType();

    void setType(Type type);

    T getDetails();

    void setDetails(T details);

    Long getTimestamp();

    void setTimestamp(Long timestamp);
}
