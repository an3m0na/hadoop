package org.apache.hadoop.tools.posum.simulator.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.database.client.DataStoreInterface;

/**
 * Created by ane on 2/9/16.
 */
public abstract class JobBehaviorPredictor{

    protected Configuration conf;
    protected DataStoreInterface dataStoreInterface;

    public JobBehaviorPredictor(Configuration conf, DataStoreInterface dataStoreInterface) {
        this.conf = conf;
        this.dataStoreInterface = dataStoreInterface;
    }

    public abstract Integer predictJobDuration(String jobId);

    public abstract Integer predictTaskDuration(String jobId, TaskType type);

    public abstract Integer predictTaskDuration(String jobId, String taskId);

}
