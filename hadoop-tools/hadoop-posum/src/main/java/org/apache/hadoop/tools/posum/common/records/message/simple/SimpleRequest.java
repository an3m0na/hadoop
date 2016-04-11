package org.apache.hadoop.tools.posum.common.records.message.simple;


import org.apache.hadoop.tools.posum.common.records.message.simple.impl.pb.ConfigurationRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.message.simple.impl.pb.SimpleRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.message.simple.impl.pb.VoidRequestPBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos.SimpleRequestProto.SimpleRequestTypeProto;

/**
 * Created by ane on 3/20/16.
 */


public abstract class SimpleRequest<T> {

    public enum Type {
        CONFIG(ConfigurationRequestPBImpl.class),
        INIT(ConfigurationRequestPBImpl.class),
        REINIT(ConfigurationRequestPBImpl.class),
        START(VoidRequestPBImpl.class),
        STOP(VoidRequestPBImpl.class), NUM_NODES(VoidRequestPBImpl.class);

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
                                                   T payload,
                                                   Class<? extends SimpleRequest<T>> implClass)
            throws IllegalAccessException, InstantiationException {
        SimpleRequest<T> request = implClass.newInstance();
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
