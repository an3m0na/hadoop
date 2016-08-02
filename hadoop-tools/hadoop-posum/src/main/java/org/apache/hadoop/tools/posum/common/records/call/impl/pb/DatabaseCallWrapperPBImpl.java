package org.apache.hadoop.tools.posum.common.records.call.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.pb.ByteStringSerializable;
import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.call.GeneralDatabaseCall;
import org.apache.hadoop.tools.posum.common.records.call.DatabaseCallType;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.database.client.ExtendedDataClientInterface;
import org.apache.hadoop.yarn.proto.POSUMProtos.DatabaseCallProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.DatabaseCallProtoOrBuilder;

/**
 * Created by ane on 8/1/16.
 */
public class DatabaseCallWrapperPBImpl implements DatabaseCall {
    private DatabaseCallProto proto = DatabaseCallProto.getDefaultInstance();
    private DatabaseCallProto.Builder builder = null;
    private boolean viaProto = false;

    public DatabaseCallWrapperPBImpl() {
        builder = DatabaseCallProto.newBuilder();
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

    public DatabaseCallType getCallType() {
        DatabaseCallProtoOrBuilder p = viaProto ? proto : builder;
        return DatabaseCallType.valueOf(p.getType().name().substring("CALL_".length()));
    }

    public void setInnerCall(GeneralDatabaseCall call) {
        if (call == null)
            return;
        maybeInitBuilder();
        DatabaseCallType type = DatabaseCallType.fromMappedClass(call.getClass());
        if (type == null)
            throw new PosumException("DatabaseCall class not recognized " + call.getClass());
        builder.setType(DatabaseCallProto.CallTypeProto.valueOf("CALL_" + type.name()));
        ByteStringSerializable serializableCall = (ByteStringSerializable) call;
        builder.setCall(serializableCall.getProtoBytes());
    }

    public GeneralDatabaseCall getInnerCall() {
        DatabaseCallProtoOrBuilder p = viaProto ? proto : builder;
        Class<? extends GeneralDatabaseCall> callClass = getCallType().getMappedClass();
        try {
            GeneralDatabaseCall call = callClass.newInstance();
            ((ByteStringSerializable) call).populateFromProtoBytes(p.getCall());
            return call;
        } catch (Exception e) {
            throw new PosumException("Could not read call from byte string " + p.getCall() + " as " + callClass, e);
        }
    }

    @Override
    public Payload executeCall(ExtendedDataClientInterface dataStore) {
        return getInnerCall().executeCall(dataStore);
    }
}
