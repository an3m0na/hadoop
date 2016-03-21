package org.apache.hadoop.tools.posum.simulator.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.POSUMConfiguration;
import org.apache.hadoop.tools.posum.database.store.DataCollection;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.tools.posum.database.client.DataStoreClient;
import org.apache.hadoop.tools.posum.simulator.predictor.BasicPredictor;
import org.apache.hadoop.tools.posum.simulator.predictor.JobBehaviorPredictor;

import java.lang.reflect.InvocationTargetException;

/**
 * Created by ane on 2/4/16.
 */
public class SimulatorMaster extends CompositeService {

    DataStoreClient dataStore;


    public SimulatorMaster() {
        super(SimulatorMaster.class.getName());
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        dataStore = new DataStoreClient();
        dataStore.init(conf);
        addIfService(dataStore);


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

    @Override
    protected void serviceStart() throws Exception {
        super.serviceStart();
        dataStore.findById(DataCollection.APPS, "1234");
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.addResource("posum-core.xml");
        SimulatorMaster master = new SimulatorMaster();
        master.init(conf);
        master.start();
    }

}
