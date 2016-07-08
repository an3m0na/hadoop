package org.apache.hadoop.tools.posum.simulator.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
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

    /* WARNING! Prediction methods may throw exceptions if data model changes occur during computation (e.g. task finishes) */

    public abstract Long predictJobDuration(String jobId);

    public abstract Long predictLocalMapTaskDuration(String jobId);

    public abstract Long predictTaskDuration(String jobId, TaskType type);

    public Long predictTaskDuration(String taskId) {
        TaskProfile task = getDataStore().findById(DataEntityType.TASK, taskId);
        if (task == null)
            throw new POSUMException("Task not found for id " + taskId);
        if(task.getDuration() > 0)
            throw new POSUMException("Task has already finished: " + taskId);
        return predictTaskDuration(task.getJobId(), task.getType());
    }

    DBInterface getDataStore() {
        if (dataStore == null)
            throw new POSUMException("DataStore not initialized in Predictor");
        return dataStore;
    }
}
