package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.yarn.util.Records;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Created by ane on 4/5/16.
 */
public abstract class HandleEventResponse extends SimpleResponse {

    public static HandleEventResponse newInstance(boolean successful, String text) {
        HandleEventResponse response = Records.newRecord(HandleEventResponse.class);
        response.setSuccessful(successful);
        response.setText(text);
        return response;
    }

    public static HandleEventResponse newInstance(boolean successful) {
        return newInstance(successful, null);
    }

    public static HandleEventResponse newInstance(boolean successful, String text, Throwable e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        HandleEventResponse ret = newInstance(successful, text);
        ret.setDetails(sw.toString());
        return ret;
    }
}
