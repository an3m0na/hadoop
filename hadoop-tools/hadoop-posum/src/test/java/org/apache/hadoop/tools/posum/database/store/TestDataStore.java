package org.apache.hadoop.tools.posum.database.store;

import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.test.TestDataClientImplementations;
import org.apache.hadoop.tools.posum.test.Utils;
import org.junit.After;

/**
 * Created by ane on 3/4/16.
 */
public class TestDataStore extends TestDataClientImplementations{

    @Override
    protected void setUpDataStore() throws Exception {
        Utils.runMongoDB();
        dataStore = new DataStore(POSUMConfiguration.newInstance());
    }

    @After
    public void tearDown() throws Exception {
        Utils.stopMongoDB();
    }
}
