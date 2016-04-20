package org.apache.hadoop.tools.posum.simulator.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.database.client.DataStoreInterface;

/**
 * Created by ane on 2/9/16.
 */
public abstract class JobBehaviorPredictor {

    protected Configuration conf;
    private DataStoreInterface dataStore;

    public JobBehaviorPredictor(Configuration conf) {
        this.conf = conf;
    }

    public abstract Integer predictJobDuration(String jobId);

    public abstract Integer predictTaskDuration(String jobId, TaskType type);

    public abstract Integer predictTaskDuration(String jobId, String taskId);

    public void setDataStore(DataStoreInterface dataStore) {
        this.dataStore = dataStore;
    }

    DataStoreInterface getDataStore() {
        if (dataStore == null)
            throw new POSUMException("DataStore not initialized in Simulator");
        return dataStore;
    }
}
