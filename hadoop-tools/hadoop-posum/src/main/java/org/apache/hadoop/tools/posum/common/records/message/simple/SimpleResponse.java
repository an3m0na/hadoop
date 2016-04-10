package org.apache.hadoop.tools.posum.common.records.message.simple;

import org.apache.hadoop.tools.posum.common.records.message.simple.impl.pb.SimpleResponsePBImpl;
import org.apache.hadoop.tools.posum.common.records.message.simple.impl.pb.VoidResponsePBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos.SimpleResponseProto.SimpleResponseTypeProto;

/**
 * Created by ane on 3/31/16.
 */
public abstract class SimpleResponse<T> {

    public enum Type {
        VOID_TYPE(VoidResponsePBImpl.class);

        private Class<? extends SimpleResponsePBImpl> implClass;
        private static final String prefix = "RESP_";

        Type(Class<? extends SimpleResponsePBImpl> implClass) {
            this.implClass = implClass;
        }

        public Class<? extends SimpleResponsePBImpl> getImplClass() {
            return implClass;
        }

        public static Type fromProto(SimpleResponseTypeProto proto) {
            return Type.valueOf(proto.name().substring(prefix.length()));
        }

        public SimpleResponseTypeProto toProto() {
            return SimpleResponseTypeProto.valueOf(prefix + name());
        }
    }

    public static SimpleResponse newInstance(boolean successful, String text) {
        SimpleResponse response = new VoidResponsePBImpl();
        response.setType(Type.VOID_TYPE);
        response.setSuccessful(successful);
        response.setText(text);
        return response;
    }

    public static SimpleResponse newInstance(boolean successful) {
        return newInstance(successful, null);
    }

    public static SimpleResponse newInstance(boolean successful, String text, Throwable e) {
        SimpleResponse ret = newInstance(successful, text);
        ret.setException(e);
        return ret;
    }

    public abstract String getText();

    public abstract void setText(String text);

    public abstract Throwable getException();

    public abstract void setException(Throwable exception);

    public abstract boolean getSuccessful();

    public abstract void setSuccessful(boolean successful);

    public abstract Type getType();

    public abstract void setType(Type type);

    public abstract T getPayload();

    public abstract void setPayload(T payload);
}
