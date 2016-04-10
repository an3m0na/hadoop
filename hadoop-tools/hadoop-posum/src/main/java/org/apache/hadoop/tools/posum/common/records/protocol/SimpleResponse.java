package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.yarn.util.Records;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Created by ane on 3/31/16.
 */
public abstract class SimpleResponse {

    public static SimpleResponse newInstance(boolean successful, String text) {
        SimpleResponse response = Records.newRecord(SimpleResponse.class);
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
}
