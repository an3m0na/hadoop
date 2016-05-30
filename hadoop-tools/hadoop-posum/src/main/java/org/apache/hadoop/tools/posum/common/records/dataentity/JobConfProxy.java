package org.apache.hadoop.tools.posum.common.records.dataentity;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;

import java.util.Map;

/**
 * Created by ane on 2/8/16.
 */
public interface JobConfProxy extends GeneralDataEntity {

    String getEntry(String name);

    JobConf getConf();

    void setConf(JobConf conf);

    String getConfPath();

    void setConfPath(String confPath);
}
