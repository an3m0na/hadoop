package org.apache.hadoop.tools.posum.common.records.call.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.pb.ByteStringSerializable;
import org.apache.hadoop.tools.posum.common.records.call.GeneralDatabaseCall;
import org.apache.hadoop.tools.posum.common.records.call.TransactionCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.DataEntityDBPBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.POSUMProtos.TransactionCallProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.TransactionCallProtoOrBuilder;

import java.util.*;

/**
 * Created by ane on 3/20/16.
 */
public class TransactionCallPBImpl extends TransactionCall implements ByteStringSerializable {
    private TransactionCallProto proto = TransactionCallProto.getDefaultInstance();
    private TransactionCallProto.Builder builder = null;
    private boolean viaProto = false;

    List<GeneralDatabaseCall> calls;

    public TransactionCallPBImpl() {
        builder = TransactionCallProto.newBuilder();
    }

    public TransactionCallPBImpl(TransactionCallProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    public TransactionCallProto getProto() {
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
        builder.clearCalls();
        if (calls == null)
            return;
        final Iterable<POSUMProtos.DatabaseCallProto> iterable =
                new Iterable<POSUMProtos.DatabaseCallProto>() {

                    @Override
                    public Iterator<POSUMProtos.DatabaseCallProto> iterator() {
                        return new Iterator<POSUMProtos.DatabaseCallProto>() {

                            Iterator<GeneralDatabaseCall> iterator = calls.iterator();

                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public POSUMProtos.DatabaseCallProto next() {
                                DatabaseCallWrapperPBImpl wrapper = new DatabaseCallWrapperPBImpl();
                                wrapper.setInnerCall(iterator.next());
                                return wrapper.getProto();
                            }

                            @Override
                            public boolean hasNext() {
                                return iterator.hasNext();
                            }
                        };
                    }
                };
        builder.addAllCalls(iterable);
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
            builder = TransactionCallProto.newBuilder(proto);
        }
        viaProto = false;
    }

    @Override
    public DataEntityDB getEntityDBOrNull() {
        TransactionCallProtoOrBuilder p = viaProto ? proto : builder;
        return p.hasEntityDB() ? new DataEntityDBPBImpl(p.getEntityDB()) : null;
    }

    @Override
    public void setEntityDBOrNull(DataEntityDB db) {
        maybeInitBuilder();
        if (db != null)
            builder.setEntityDB(((DataEntityDBPBImpl) db).getProto());
    }

    @Override
    public List<GeneralDatabaseCall> getCallList() {
        if (this.calls == null) {
            TransactionCallProtoOrBuilder p = viaProto ? proto : builder;
            this.calls = new ArrayList<>(p.getCallsCount());
            for (POSUMProtos.DatabaseCallProto callProto : p.getCallsList()) {
                if (callProto != null) {
                    calls.add(new DatabaseCallWrapperPBImpl(callProto).getInnerCall());
                }
            }
        }
        return this.calls;
    }

    @Override
    public void setCallList(List<GeneralDatabaseCall> callList) {
        if (calls == null)
            return;
        this.calls = new ArrayList<>(callList);
    }

    @Override
    public TransactionCall addCall(GeneralDatabaseCall call) {
        getCallList().add(call);
        return this;
    }

    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        this.proto = TransactionCallProto.parseFrom(data);
        viaProto = true;
    }
}