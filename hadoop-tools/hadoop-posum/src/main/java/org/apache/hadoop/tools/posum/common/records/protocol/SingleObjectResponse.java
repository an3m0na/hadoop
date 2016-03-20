package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/20/16.
 */
public abstract class SingleObjectResponse {

    public static SingleObjectResponse newInstance(String responseString) {
        SingleObjectResponse response = Records.newRecord(SingleObjectResponse.class);
        response.setResponse(responseString);
        return response;
    }

    public abstract Object getResponse();

    public abstract void setResponse(String response);

}
