package org.apache.hadoop.tools.posum.simulator.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.data.mock.data.MockDataStoreImpl;
import org.junit.Before;

import java.lang.reflect.InvocationTargetException;

public abstract class TestPredictor<T extends JobBehaviorPredictor> {
    protected Configuration conf = PosumConfiguration.newInstance();
    protected T predictor;
    protected Class<T> predictorClass;


    public TestPredictor(Class<T> predictorClass) {
        this.predictorClass = predictorClass;
    }

    @Before
    public void setUp() throws Exception {
        MockDataStoreImpl dataStore = new MockDataStoreImpl();

        try {
            predictor = predictorClass.getConstructor(Configuration.class, Database.class)
                    .newInstance(conf, Database.extractFrom(dataStore, DataEntityDB.getMain()));
        } catch (NoSuchMethodException |
                InvocationTargetException |
                InstantiationException |
                IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
