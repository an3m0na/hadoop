package org.apache.hadoop.tools.posum.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.database.client.DBInterface;
import org.apache.hadoop.tools.posum.simulator.predictor.BasicPredictor;
import org.apache.hadoop.tools.posum.simulator.predictor.JobBehaviorPredictor;
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

    private JobBehaviorPredictor initPredictor(DBInterface dbInterface) {

        conf = TestUtils.getConf();

        Class<? extends JobBehaviorPredictor> predictorClass = conf.getClass(
                POSUMConfiguration.PREDICTOR_CLASS,
                BasicPredictor.class,
                JobBehaviorPredictor.class
        );

        JobBehaviorPredictor predictor = null;
        try {
            predictor = predictorClass.getConstructor(Configuration.class, DBInterface.class)
                    .newInstance(conf, dbInterface);
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
        MockDataMasterClient dataStore = new MockDataMasterClient();
        try {
            URL traceUrl = getClass().getClassLoader().getResource("2jobs2min-rumen-jh.json");
            if (traceUrl == null)
                throw new RuntimeException("Trace file not found");
            dataStore.populateFromTrace(traceUrl.getFile());
        } catch (IOException e) {
            e.printStackTrace();
        }
        JobBehaviorPredictor predictor = initPredictor(dataStore.bindTo(DataEntityDB.getMain()));
        System.out.println(dataStore.getJobList());
        int heartbeat = conf.getInt(POSUMConfiguration.MASTER_HEARTBEAT_MS,
                POSUMConfiguration.MASTER_HEARTBEAT_MS_DEFAULT);
        for (long i = 100000; i < dataStore.getSimulationTime(); i += heartbeat) {
            dataStore.setCurrentTime(i);
            System.out.println("# ------------ TIME : " + i + " ------------ #");
            for (Map.Entry<String, List<String>> job : dataStore.getFutureJobInfo().entrySet()) {
                StringBuilder recordBuilder = new StringBuilder(job.getValue().size() + 3);
                recordBuilder
                        .append(job.getKey()).append("=").append(predictor.predictJobDuration(job.getKey()))
                        .append("\t")
                        .append("MAP=").append(predictor.predictTaskDuration(job.getKey(), TaskType.MAP))
                        .append("\t")
                        .append("RED=").append(predictor.predictTaskDuration(job.getKey(), TaskType.REDUCE))
                        .append("\t");
                for (String task : job.getValue())
                    recordBuilder
                            .append(task).append("=").append(predictor.predictTaskDuration(task))
                            .append("\t");
                recordBuilder.append("\n");
                System.out.println(recordBuilder.toString());
            }
        }

    }
}
