package org.apache.hadoop.tools.posum.common.records.call.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.call.StoreLogCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.LogEntryPBImpl;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos.LogEntryProto;

/**
 * Created by ane on 3/20/16.
 */
public class StoreLogCallPBImpl extends StoreLogCall implements PayloadPB {
    private LogEntryProto proto = LogEntryProto.getDefaultInstance();

    public StoreLogCallPBImpl() {
    }

    public StoreLogCallPBImpl(LogEntryProto proto) {
        this.proto = proto;
    }

    public LogEntryProto getProto() {
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

    @Override
    public LogEntry getLogEntry() {
        return new LogEntryPBImpl(proto);
    }

    @Override
    public void setLogEntry(LogEntry logEntry) {
        proto = ((LogEntryPBImpl<?>) logEntry).getProto();
    }

    @Override
    public ByteString getProtoBytes() {
        return getProto().toByteString();
    }

    @Override
    public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
        this.proto = LogEntryProto.parseFrom(data);
    }
}
