package org.apache.hadoop.tools.posum.simulator.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.database.client.Database;
import org.apache.hadoop.tools.posum.database.mock.MockDataStoreImpl;
import org.junit.Before;

import java.lang.reflect.InvocationTargetException;

/**
 * Created by ane on 2/10/16.
 */
public abstract class TestPredictor<T extends JobBehaviorPredictor> {
    protected Configuration conf = PosumConfiguration.newInstance();
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
            predictor = predictorClass.getConstructor(Configuration.class, Database.class)
                    .newInstance(conf, dataStore.bindTo(DataEntityDB.getMain()));
        } catch (NoSuchMethodException |
                InvocationTargetException |
                InstantiationException |
                IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
