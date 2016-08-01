package org.apache.hadoop.tools.posum.common.records.payload;

/**
 * Created by ane on 7/30/16.
 */
public class VoidPayload implements Payload{

    public static VoidPayload newInstance() {
        return new VoidPayload();
    }

    private VoidPayload(){

    }
}
