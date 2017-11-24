package org.apache.hadoop.tools.posum.common.records.request;


import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.payload.PayloadType;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.records.payload.VoidPayload;
import org.apache.hadoop.yarn.proto.PosumProtos.SimpleRequestProto.SimpleRequestTypeProto;
import org.apache.hadoop.yarn.util.Records;


public abstract class SimpleRequest<T extends Payload> {

  public enum Type {
    PING(PayloadType.SIMPLE_PROPERTY),
    CHANGE_POLICY(PayloadType.SIMPLE_PROPERTY),
    START(PayloadType.VOID),
    SYSTEM_ADDRESSES(PayloadType.VOID),
    LIST_COLLECTIONS(PayloadType.VOID),
    CLEAR_DATA(PayloadType.VOID),
    CLEAR_DB(PayloadType.DB_ALTERATION),
    COPY_DB(PayloadType.DB_ALTERATION),
    COPY_COLL(PayloadType.DB_ALTERATION),
    AWAIT_UPDATE(PayloadType.DB_LOCK),
    NOTIFY_UPDATE(PayloadType.DB_LOCK),
    RESET(PayloadType.VOID);

    private static final String prefix = "REQ_";
    private PayloadType payloadType;

    Type(PayloadType payloadType) {
      this.payloadType = payloadType;
    }

    public PayloadType getPayloadType() {
      return payloadType;
    }

    public static Type fromProto(SimpleRequestTypeProto proto) {
      return Type.valueOf(proto.name().substring(prefix.length()));
    }

    public SimpleRequestTypeProto toProto() {
      return SimpleRequestTypeProto.valueOf(prefix + name());
    }
  }

  public static <T extends Payload> SimpleRequest<T> newInstance(Type type,
                                                                 T payload) {
    SimpleRequest<T> request = Records.newRecord(SimpleRequest.class);
    request.setType(type);
    request.setPayload(payload);
    return request;
  }

  public static SimpleRequest<SimplePropertyPayload> newInstance(Type type, String payload) {
    return newInstance(type, SimplePropertyPayload.newInstance("payload", payload));
  }

  public static SimpleRequest<VoidPayload> newInstance(Type type) {
    return newInstance(type, VoidPayload.newInstance());
  }

  public abstract Type getType();

  public abstract void setType(Type type);

  public abstract T getPayload();

  public abstract void setPayload(T payload);
}
