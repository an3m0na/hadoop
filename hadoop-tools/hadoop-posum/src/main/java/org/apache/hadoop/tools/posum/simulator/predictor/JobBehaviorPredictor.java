package org.apache.hadoop.tools.posum.simulator.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.database.client.DataBroker;

/**
 * Created by ane on 2/9/16.
 */
public abstract class JobBehaviorPredictor {

    protected Configuration conf;
    private DataBroker dataBroker;

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

    public void initialize(DataBroker dataBroker) {
        this.dataBroker = dataBroker;
    }

    /* WARNING! Prediction methods may throw exceptions if data model changes occur during computation (e.g. task finishes) */

    public abstract Long predictJobDuration(String jobId);

    public abstract Long predictTaskDuration(String jobId, TaskType type);

    public Long predictTaskDuration(String taskId) {
        FindByIdCall getTask = FindByIdCall.newInstance(DataEntityCollection.TASK, taskId);
        TaskProfile task = getDataBroker().executeDatabaseCall(getTask).getEntity();
        if (task == null)
            throw new PosumException("Task not found for id " + taskId);
        if(task.getDuration() > 0)
            throw new PosumException("Task has already finished: " + taskId);
        return predictTaskDuration(task.getJobId(), task.getType());
    }

    DataBroker getDataBroker() {
        if (dataBroker == null)
            throw new PosumException("DataBroker not initialized in Predictor");
        return dataBroker;
    }
}
