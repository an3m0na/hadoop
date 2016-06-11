package org.apache.hadoop.tools.posum.common.records.dataentity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.hadoop.mapred.JobConf;

import java.util.Map;

/**
 * Created by ane on 2/8/16.
 */
public interface JobConfProxy extends GeneralDataEntity {

    @JsonIgnore
    String getEntry(String name);

    @JsonIgnore
    JobConf getConf();

    void setConf(JobConf conf);

    String getConfPath();

    void setConfPath(String confPath);

    Map<String, String> getPropertyMap();

    void setPropertyMap(Map<String, String> propertyMap);
}
