package org.apache.hadoop.tools.posum.common.records.request;


import org.apache.hadoop.tools.posum.common.records.request.impl.pb.*;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.yarn.proto.PosumProtos.SimpleRequestProto.SimpleRequestTypeProto;

/**
 * Created by ane on 3/20/16.
 */


public abstract class SimpleRequest<T> {

    public enum Type {
        PING(StringRequestPBImpl.class),
        CHANGE_POLICY(StringRequestPBImpl.class),
        START(VoidRequestPBImpl.class),
        LOG_POLICY_CHANGE(StringRequestPBImpl.class),
        SYSTEM_ADDRESSES(VoidRequestPBImpl.class);

        private Class<? extends SimpleRequestPBImpl> implClass;
        private static final String prefix = "REQ_";

        Type(Class<? extends SimpleRequestPBImpl> implClass) {
            this.implClass = implClass;
        }

        public Class<? extends SimpleRequestPBImpl> getImplClass() {
            return implClass;
        }

        public static Type fromProto(SimpleRequestTypeProto proto) {
            return Type.valueOf(proto.name().substring(prefix.length()));
        }

        public SimpleRequestTypeProto toProto() {
            return SimpleRequestTypeProto.valueOf(prefix + name());
        }
    }

    public static <T> SimpleRequest<T> newInstance(Type type,
                                                   T payload) {
        SimpleRequest<T> request;
        try {
            request = type.getImplClass().newInstance();
        } catch (InstantiationException | IllegalAccessException ex) {
            throw new PosumException("Could not instantiate request of type " + type, ex);
        }
        request.setType(type);
        request.setPayload(payload);
        return request;
    }

    public static VoidRequestPBImpl newInstance(Type type) {
        VoidRequestPBImpl request = new VoidRequestPBImpl();
        request.setType(type);
        return request;
    }

    public abstract Type getType();

    public abstract void setType(Type type);

    public abstract T getPayload();

    public abstract void setPayload(T payload);


}
