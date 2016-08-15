package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.yarn.proto.PosumProtos;
import org.apache.hadoop.yarn.proto.PosumProtos.LogEntryProto;
import org.apache.hadoop.yarn.proto.PosumProtos.LogEntryProtoOrBuilder;
import org.bson.types.ObjectId;

/**
 * Created by ane on 3/21/16.
 */
public class LogEntryPBImpl<T extends Payload> extends GeneralDataEntityPBImpl<LogEntry<T>, LogEntryProto, LogEntryProto.Builder>
        implements LogEntry<T> {

    private T details;

    public LogEntryPBImpl(LogEntryProto proto) {
        super(proto);
    }

    public LogEntryPBImpl() {
        setId(ObjectId.get().toHexString());
        setTimestamp(System.currentTimeMillis());
    }

    public LogEntryPBImpl(Type type, T details) {
        setType(type);
        setDetails(details);
    }

    @Override
    void initBuilder() {
        builder = viaProto ? LogEntryProto.newBuilder(proto) : LogEntryProto.newBuilder();
    }

    @Override
    void buildProto() {
        if (details != null)
            builder.setDetails(((PayloadPB) details).getProtoBytes());
        proto = builder.build();
    }

    @Override
    public LogEntry parseToEntity(ByteString data) throws InvalidProtocolBufferException {
        this.proto = LogEntryProto.parseFrom(data);
        viaProto = true;
        return this;
    }

    @Override
    public String getId() {
        LogEntryProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasId())
            return null;
        return p.getId();
    }

    @Override
    public void setId(String id) {
        maybeInitBuilder();
        if (id == null) {
            builder.clearId();
            return;
        }
        builder.setId(id);
    }

    @Override
    public Long getTimestamp() {
        LogEntryProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasTimestamp())
            return null;
        return p.getTimestamp();
    }

    @Override
    public void setTimestamp(Long timestamp) {
        maybeInitBuilder();
        if (timestamp == null) {
            builder.clearTimestamp();
            return;
        }
        builder.setTimestamp(timestamp);
    }

    @Override
    public LogEntry.Type getType() {
        LogEntryProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasType())
            return null;
        return LogEntry.Type.valueOf(p.getType().name().substring("LG_".length()));
    }

    @Override
    public void setType(LogEntry.Type type) {
        maybeInitBuilder();
        if (type == null) {
            builder.clearType();
            return;
        }
        builder.setType(PosumProtos.LogEntryProto.LogTypeProto.valueOf("LG_" + type.name()));
    }

    @Override
    public T getDetails() {
        if (details == null) {
            LogEntryProtoOrBuilder p = viaProto ? proto : builder;
            if (p.hasDetails())
                try {
                    details = (T) getType().getDetailsType().getImplClass().newInstance();
                    ((PayloadPB) details).populateFromProtoBytes(p.getDetails());
                } catch (InstantiationException | IllegalAccessException | InvalidProtocolBufferException e) {
                    throw new PosumException("Could not read message payload", e);
                }
        }
        return details;
    }

    @Override
    public void setDetails(T details) {
        maybeInitBuilder();
        this.details = details;
    }

}
