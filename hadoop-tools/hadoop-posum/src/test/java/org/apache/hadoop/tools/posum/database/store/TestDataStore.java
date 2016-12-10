package org.apache.hadoop.tools.posum.database.store;

import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.test.IntegrationTest;
import org.apache.hadoop.tools.posum.test.TestDataBroker;
import org.apache.hadoop.tools.posum.test.Utils;
import org.junit.After;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
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
