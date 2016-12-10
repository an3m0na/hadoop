package org.apache.hadoop.tools.posum.simulator.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumException;

public abstract class JobBehaviorPredictor {

    protected Configuration conf;
    private Database db;

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

    public void initialize(Database db) {
        this.db = db;
    }

    /* WARNING! Prediction methods may throw exceptions if data model changes occur during computation (e.g. task finishes) */

    public abstract Long predictJobDuration(String jobId);

    public abstract Long predictTaskDuration(String jobId, TaskType type);

    public Long predictTaskDuration(String taskId) {
        FindByIdCall getTask = FindByIdCall.newInstance(DataEntityCollection.TASK, taskId);
        TaskProfile task = getDatabase().executeDatabaseCall(getTask).getEntity();
        if (task == null)
            throw new PosumException("Task not found for id " + taskId);
        if(task.getDuration() > 0)
            throw new PosumException("Task has already finished: " + taskId);
        return predictTaskDuration(task.getJobId(), task.getType());
    }

    Database getDatabase() {
        if (db == null)
            throw new PosumException("Database not initialized in Predictor");
        return db;
    }
}
