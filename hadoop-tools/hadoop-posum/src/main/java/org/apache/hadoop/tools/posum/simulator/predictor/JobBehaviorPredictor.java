package org.apache.hadoop.tools.posum.simulator.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.database.client.DBInterface;

/**
 * Created by ane on 2/9/16.
 */
public abstract class JobBehaviorPredictor {

    protected Configuration conf;
    private DBInterface dataStore;

    public static JobBehaviorPredictor newInstance(Configuration conf) {
        return newInstance(conf, conf.getClass(
                PosumConfiguration.PREDICTOR_CLASS,
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
            throw new PosumException("Could not instantiate predictor type " + predictorClass.getName(), e);
        }
    }

    public void initialize(DBInterface dataStore) {
        this.dataStore = dataStore;
    }

    /* WARNING! Prediction methods may throw exceptions if data model changes occur during computation (e.g. task finishes) */

    public abstract Long predictJobDuration(String jobId);

    public abstract Long predictTaskDuration(String jobId, TaskType type);

    public Long predictTaskDuration(String taskId) {
        TaskProfile task = getDataStore().findById(DataEntityCollection.TASK, taskId);
        if (task == null)
            throw new PosumException("Task not found for id " + taskId);
        if(task.getDuration() > 0)
            throw new PosumException("Task has already finished: " + taskId);
        return predictTaskDuration(task.getJobId(), task.getType());
    }

    DBInterface getDataStore() {
        if (dataStore == null)
            throw new PosumException("DataStore not initialized in Predictor");
        return dataStore;
    }
}
