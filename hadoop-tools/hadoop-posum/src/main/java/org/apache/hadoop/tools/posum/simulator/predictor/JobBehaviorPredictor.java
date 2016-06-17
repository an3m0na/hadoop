package org.apache.hadoop.tools.posum.simulator.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.database.client.DBInterface;

/**
 * Created by ane on 2/9/16.
 */
public abstract class JobBehaviorPredictor {

    protected Configuration conf;
    private DBInterface dataStore;

    public static JobBehaviorPredictor newInstance(Configuration conf) {
        return newInstance(conf, conf.getClass(
                POSUMConfiguration.PREDICTOR_CLASS,
                BasicPredictor.class,
                JobBehaviorPredictor.class
        ));
    }

    public static JobBehaviorPredictor newInstance(Configuration conf,
                                                   Class<? extends JobBehaviorPredictor> predictorClass) {
        try {
            JobBehaviorPredictor predictor = predictorClass.newInstance();
            predictor.conf = conf;
            return predictor;
        } catch (Exception e) {
            throw new POSUMException("Could not instantiate predictor type " + predictorClass.getName(), e);
        }
    }

    public void initialize(DBInterface dataStore) {
        this.dataStore = dataStore;
    }

    public abstract Long predictJobDuration(String jobId);

    public abstract Long predictLocalMapTaskDuration(String jobId);

    public abstract Long predictTaskDuration(String jobId, TaskType type);

    public abstract Long predictTaskDuration(String taskId);

    DBInterface getDataStore() {
        if (dataStore == null)
            throw new POSUMException("DataStore not initialized in Predictor");
        return dataStore;
    }
}
