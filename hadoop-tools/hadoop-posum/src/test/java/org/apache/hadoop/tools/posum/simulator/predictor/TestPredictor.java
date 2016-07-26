package org.apache.hadoop.tools.posum.simulator.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.database.client.DBInterface;
import org.apache.hadoop.tools.posum.test.MockDataMasterClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/10/16.
 */
public abstract class TestPredictor<T extends JobBehaviorPredictor> {
    protected Configuration conf = POSUMConfiguration.newInstance();
    protected T predictor;
    protected MockDataMasterClient dataStore;
    protected Class<T> predictorClass;


    public TestPredictor(Class<T> predictorClass) {
        this.predictorClass = predictorClass;
    }

    @Before
    public void setUp() throws Exception {
        MockDataMasterClient dataStore = new MockDataMasterClient();
        try {
            URL traceUrl = getClass().getClassLoader().getResource("2jobs2min-rumen-jh.json");
            if (traceUrl == null)
                throw new RuntimeException("Trace file not found");
            dataStore.populateFromTrace(traceUrl.getFile());
            System.out.println(dataStore.getJobList());
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            predictor = predictorClass.getConstructor(Configuration.class, DBInterface.class)
                    .newInstance(conf, dataStore.bindTo(DataEntityDB.getMain()));
        } catch (NoSuchMethodException |
                InvocationTargetException |
                InstantiationException |
                IllegalAccessException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testPredictorAccuracy() {
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
