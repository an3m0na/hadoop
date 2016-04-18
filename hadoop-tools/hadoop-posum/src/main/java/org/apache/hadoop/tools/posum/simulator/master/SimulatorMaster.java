package org.apache.hadoop.tools.posum.simulator.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.database.store.DataStoreInterface;
import org.apache.hadoop.tools.posum.database.client.DataMasterClient;
import org.apache.hadoop.tools.posum.simulator.predictor.BasicPredictor;
import org.apache.hadoop.tools.posum.simulator.predictor.JobBehaviorPredictor;

import java.lang.reflect.InvocationTargetException;

/**
 * Created by ane on 2/4/16.
 */
public class SimulatorMaster extends CompositeService {

    DataMasterClient dataStore;


    public SimulatorMaster() {
        super(SimulatorMaster.class.getName());
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        dataStore = new DataMasterClient();
        dataStore.init(conf);
        addIfService(dataStore);


        Class<? extends JobBehaviorPredictor> predictorClass = conf.getClass(
                POSUMConfiguration.PREDICTOR_CLASS,
                BasicPredictor.class,
                JobBehaviorPredictor.class
        );

        JobBehaviorPredictor predictor;
        try {
            predictor = predictorClass.getConstructor(DataStoreInterface.class).newInstance(dataStore);
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
    }

    public static void main(String[] args) {
        Configuration conf = POSUMConfiguration.newInstance();
        SimulatorMaster master = new SimulatorMaster();
        master.init(conf);
        master.start();
    }

}