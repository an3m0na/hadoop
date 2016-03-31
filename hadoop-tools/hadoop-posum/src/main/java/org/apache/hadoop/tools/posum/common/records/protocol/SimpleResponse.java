package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/31/16.
 */
public abstract class SimpleResponse {

    public static SimpleResponse newInstance(boolean successful, String text, String details) {
        SimpleResponse response = Records.newRecord(SimpleResponse.class);
        response.setSuccessful(successful);
        response.setText(text);
        response.setDetails(details);
        return response;
    }

    public static SimpleResponse newInstance(boolean successful, String text) {
        return newInstance(successful, text, null);
    }

    public abstract String getText();

    public abstract void setText(String text);

    public abstract String getDetails();

    public abstract void setDetails(String details);

    public abstract boolean getSuccessful();

    public abstract void setSuccessful(boolean successful);
}
