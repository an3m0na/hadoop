package org.apache.hadoop.tools.posum.data.core;

import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.test.IntegrationTest;
import org.apache.hadoop.tools.posum.client.data.TestDataStore;
import org.apache.hadoop.tools.posum.test.Utils;
import org.junit.After;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class TestDataStoreImpl extends TestDataStore {

    @Override
    protected void setUpDataStore() throws Exception {
        Utils.runMongoDB();
        dataStore = new DataStoreImpl(PosumConfiguration.newInstance());
    }

    @After
    public void tearDown() throws Exception {
        Utils.stopMongoDB();
    }
}
