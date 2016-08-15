package org.apache.hadoop.tools.posum.common.records.call.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.call.DatabaseCallType;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.yarn.proto.PosumProtos.DatabaseCallProto;
import org.apache.hadoop.yarn.proto.PosumProtos.DatabaseCallProtoOrBuilder;

/**
 * Created by ane on 8/1/16.
 */
public class DatabaseCallWrapperPBImpl implements PayloadPB {
    private DatabaseCallProto proto = DatabaseCallProto.getDefaultInstance();
    private DatabaseCallProto.Builder builder = null;
    private boolean viaProto = false;

    public DatabaseCallWrapperPBImpl() {
        builder = DatabaseCallProto.newBuilder();
    }

    private DatabaseCall call;

    public DatabaseCallWrapperPBImpl(DatabaseCall call) {
        this();
        setCall(call);
    }

    public DatabaseCallWrapperPBImpl(DatabaseCallProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public DatabaseCallProto getProto() {
        mergeLocalToProto();
        proto = viaProto ? proto : builder.build();
        viaProto = true;
        return proto;
    }

    @Override
    public int hashCode() {
        return getProto().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;
        if (other.getClass().isAssignableFrom(this.getClass())) {
            return this.getProto().equals(this.getClass().cast(other).getProto());
        }
        return false;
    }

    @Override
    public String toString() {
        return TextFormat.shortDebugString(getProto());
    }

    private void mergeLocalToBuilder() {
        maybeInitBuilder();
        if (call != null) {
            DatabaseCallType type = DatabaseCallType.fromMappedClass(call.getClass());
            if (type == null)
                throw new PosumException("DatabaseCall class not recognized " + call.getClass());
            builder.setType(DatabaseCallProto.CallTypeProto.valueOf("CALL_" + type.name()));
            PayloadPB serializableCall = (PayloadPB) call;
            builder.setCall(serializableCall.getProtoBytes());
        }
    }

    private void mergeLocalToProto() {
        if (viaProto)
            maybeInitBuilder();
        mergeLocalToBuilder();
        proto = builder.build();
        viaProto = true;
    }

    private void maybeInitBuilder() {
        if (viaProto || builder == null) {
            builder = DatabaseCallProto.newBuilder(proto);
        }
        viaProto = false;
    }

    public void setCall(DatabaseCall call) {
        maybeInitBuilder();
        this.call = call;
    }

    public DatabaseCall getCall() {
        if (this.call == null) {
            DatabaseCallProtoOrBuilder p = viaProto ? proto : builder;
            DatabaseCallType type = DatabaseCallType.valueOf(p.getType().name().substring("CALL_".length()));
            Class<? extends DatabaseCall> callClass = type.getMappedClass();
            try {
                this.call = callClass.newInstance();
                ((PayloadPB) call).populateFromProtoBytes(p.getCall());
            } catch (Exception e) {
                throw new PosumException("Could not read call from byte string " + p.getCall() + " as " + callClass, e);
            }
        }
        return this.call;
    }

    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        proto = DatabaseCallProto.parseFrom(data);
        viaProto = true;
    }
}
