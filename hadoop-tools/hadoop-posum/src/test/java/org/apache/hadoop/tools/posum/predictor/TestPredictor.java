package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.POSUMConfiguration;
import org.apache.hadoop.tools.posum.database.DataStore;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by ane on 2/10/16.
 */
public class TestPredictor {


    private void initPredictor(DataStore dataStore) {
        Configuration conf;
        conf = new Configuration(false);
        conf.addResource("posum-core.xml");

        Class<? extends JobBehaviorPredictor> predictorClass = conf.getClass(
                POSUMConfiguration.PREDICTOR_CLASS,
                BasicPredictor.class,
                JobBehaviorPredictor.class
        );

        JobBehaviorPredictor predictor;
        try {
            predictor = predictorClass.getConstructor(DataStore.class).newInstance(dataStore);
            predictor.setConf(conf);
        } catch (NoSuchMethodException |
                InvocationTargetException |
                InstantiationException |
                IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPredictorAccuracy() {
        MockDataStoreClient dataStore = new MockDataStoreClient();
        try {
            dataStore.populateFromTrace("2jobs2min-rumen-jh.json");
        } catch (IOException e) {
            e.printStackTrace();
        }
        initPredictor(dataStore);
        System.out.println(dataStore.getJobList());

    }
}
