package org.apache.hadoop.tools.posum.common.records.dataentity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.hadoop.tools.posum.common.records.field.impl.pb.TaskPredictionPBImpl;
import org.apache.hadoop.tools.posum.common.util.PolicyMap;
import org.bson.types.ObjectId;
import org.mongojack.Id;

/**
 * Created by ane on 3/7/16.
 */
@JsonDeserialize(using = LogEntryDeserializer.class)
public class LogEntry<T> implements GeneralDataEntity {

    public enum Type {
        POLICY_CHANGE(String.class, DataEntityCollection.LOG_SCHEDULER),
        POLICY_MAP(PolicyMap.class, DataEntityCollection.POSUM_STATS),
        TASK_PREDICTION(TaskPredictionPBImpl.class, DataEntityCollection.LOG_PREDICTOR);

        @JsonIgnore
        private Class detailsClass;
        @JsonIgnore
        private DataEntityCollection collection;

        Type(Class detailsClass, DataEntityCollection collection) {
            this.detailsClass = detailsClass;
            this.collection = collection;
        }

        public DataEntityCollection getCollection() {
            return collection;
        }

        public Class getDetailsClass() {
            return detailsClass;
        }
    }

    @Id
    private String id;
    private Type type;
    private T details;
    private Long timestamp;

    public LogEntry(Type type, T details) {
        this.type = type;
        this.timestamp = System.currentTimeMillis();
        this.details = details;
        this.id = ObjectId.get().toHexString();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public T getDetails() {
        return details;
    }

    public void setDetails(T details) {
        this.details = details;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
