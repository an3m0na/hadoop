package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

/**
 * Created by ane on 3/31/16.
 */
public abstract class ConfigurationRequest {
    public static ConfigurationRequest newInstance(Map<String,String> properties) {
        ConfigurationRequest request = Records.newRecord(ConfigurationRequest.class);
        request.setProperties(properties);
        return request;
    }

    public abstract Map<String, String> getProperties();

    public abstract void setProperties(Map<String, String> properties);
}
