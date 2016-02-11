package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.tools.posum.POSUMConfiguration;
import org.apache.hadoop.tools.posum.database.DataStore;
import org.apache.hadoop.tools.posum.database.records.JobProfile;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/10/16.
 */
public class TestPredictor {
    Configuration conf;

    private JobBehaviorPredictor initPredictor(DataStore dataStore) {

        conf = new Configuration(false);
        conf.addResource("posum-core.xml");

        Class<? extends JobBehaviorPredictor> predictorClass = conf.getClass(
                POSUMConfiguration.PREDICTOR_CLASS,
                BasicPredictor.class,
                JobBehaviorPredictor.class
        );

        JobBehaviorPredictor predictor = null;
        try {
            predictor = predictorClass.getConstructor(DataStore.class).newInstance(dataStore);
            predictor.setConf(conf);
        } catch (NoSuchMethodException |
                InvocationTargetException |
                InstantiationException |
                IllegalAccessException e) {
            e.printStackTrace();
        }
        return predictor;
    }

    @Test
    public void testPredictorAccuracy() {
        MockDataStoreClient dataStore = new MockDataStoreClient();
        try {
            URL traceUrl = getClass().getClassLoader().getResource("2jobs2min-rumen-jh.json");
            if (traceUrl == null)
                throw new RuntimeException("Trace file not found");
            dataStore.populateFromTrace(traceUrl.getFile());
        } catch (IOException e) {
            e.printStackTrace();
        }
        JobBehaviorPredictor predictor = initPredictor(dataStore);
        System.out.println(dataStore.getJobList());
        int heartbeat = conf.getInt(POSUMConfiguration.MASTER_HEARTBEAT_MS,
                POSUMConfiguration.MASTER_HEARTBEAT_MS_DEFAULT);
        for (long i = 0; i < dataStore.getSimulationTime(); i += heartbeat) {
            dataStore.setCurrentTime(i);
            System.out.println("# ------------ TIME : i ------------ #");
            for (Map.Entry<JobID, List<TaskID>> job : dataStore.getFutureJobInfo().entrySet()) {
                StringBuilder recordBuilder = new StringBuilder(job.getValue().size() + 3);
                recordBuilder
                        .append(job.getKey()).append("=").append(predictor.predictJobDuration(job.getKey()))
                        .append("\t")
                        .append("MAP=").append(predictor.predictTaskDuration(job.getKey(), TaskType.MAP))
                        .append("\t")
                        .append("RED=").append(predictor.predictTaskDuration(job.getKey(), TaskType.REDUCE))
                        .append("\t");
                for (TaskID task : job.getValue())
                    recordBuilder
                            .append(task).append("=").append(predictor.predictTaskDuration(job.getKey(), task))
                            .append("\t");
                recordBuilder.append("\n");
                System.out.println(recordBuilder.toString());
            }
        }

    }
}
