package org.apache.hadoop.tools.posum.common.records.response;

import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.payload.PayloadType;
import org.apache.hadoop.tools.posum.common.records.response.impl.pb.*;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/31/16.
 */
public abstract class SimpleResponse<T extends Payload> {

    public static SimpleResponse newInstance(boolean successful, String text) {
        SimpleResponse response = new VoidResponsePBImpl();
        response.setType(PayloadType.VOID);
        response.setSuccessful(successful);
        response.setText(text);
        return response;
    }

    public static SimpleResponse newInstance(boolean successful) {
        return newInstance(successful, null);
    }

    public static SimpleResponse newInstance(String text, Throwable e) {
        SimpleResponse ret = newInstance(false, text);
        if (e != null)
            ret.setException(Utils.getErrorTrace(e));
        return ret;
    }

    public static <T extends Payload> SimpleResponse<T> newInstance(PayloadType payloadType,
                                                    T payload) {
        SimpleResponse<T> response = Records.newRecord(SimpleResponse.class);
        response.setSuccessful(true);
        response.setType(payloadType);
        response.setPayload(payload);
        return response;
    }

    public static <T extends Payload> SimpleResponse<T> newInstance(PayloadType payloadType, String text, Throwable e) {
        SimpleResponse<T> response = Records.newRecord(SimpleResponse.class);
        response.setSuccessful(false);
        response.setType(payloadType);
        response.setText(text);
        if (e != null)
            response.setException(Utils.getErrorTrace(e));
        return response;
    }

    public abstract String getText();

    public abstract void setText(String text);

    public abstract String getException();

    public abstract void setException(String exception);

    public abstract boolean getSuccessful();

    public abstract void setSuccessful(boolean successful);

    public abstract PayloadType getType();

    public abstract void setType(PayloadType payloadType);

    public abstract T getPayload();

    public abstract void setPayload(T payload);
}
