package org.apache.hadoop.tools.posum.simulator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.POSUMConfiguration;
import org.apache.hadoop.tools.posum.database.DataStore;
import org.apache.hadoop.tools.posum.database.DataStoreClient;
import org.apache.hadoop.tools.posum.database.MockDataStoreClient;
import org.apache.hadoop.tools.posum.predictor.BasicPredictor;
import org.apache.hadoop.tools.posum.predictor.JobBehaviorPredictor;

import java.lang.reflect.InvocationTargetException;

/**
 * Created by ane on 2/4/16.
 */
public class SimulatorMaster {

    Configuration conf;
    DataStoreClient dataStore;

    public SimulatorMaster() {
        //TODO change to actually connect to database
        dataStore = new MockDataStoreClient();
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

}
