package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 7/30/16.
 */
public abstract class VoidPayload implements Payload {

    public static VoidPayload newInstance() {
        return Records.newRecord(VoidPayload.class);
    }
}
