package org.apache.hadoop.tools.posum.simulator.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.database.client.DBInterface;
import org.apache.hadoop.tools.posum.test.MockDataStoreImpl;
import org.junit.Before;

import java.lang.reflect.InvocationTargetException;

/**
 * Created by ane on 2/10/16.
 */
public abstract class TestPredictor<T extends JobBehaviorPredictor> {
    protected Configuration conf = POSUMConfiguration.newInstance();
    protected T predictor;
    protected MockDataStoreImpl dataStore;
    protected Class<T> predictorClass;


    public TestPredictor(Class<T> predictorClass) {
        this.predictorClass = predictorClass;
    }

    @Before
    public void setUp() throws Exception {
        MockDataStoreImpl dataStore = new MockDataStoreImpl();

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
}
