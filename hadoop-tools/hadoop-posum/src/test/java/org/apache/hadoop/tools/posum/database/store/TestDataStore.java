package org.apache.hadoop.tools.posum.database.store;

import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.test.TestDataBroker;
import org.apache.hadoop.tools.posum.test.Utils;
import org.junit.After;

/**
 * Created by ane on 3/4/16.
 */
public class TestDataStore extends TestDataBroker {

    @Override
    protected void setUpDataBroker() throws Exception {
        Utils.runMongoDB();
        dataBroker = org.apache.hadoop.tools.posum.common.util.Utils.exposeDataStoreAsBroker(
                new DataStoreImpl(PosumConfiguration.newInstance())
        );
    }

    @After
    public void tearDown() throws Exception {
        Utils.stopMongoDB();
    }
}
