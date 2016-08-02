package org.apache.hadoop.tools.posum.database.mock;

import org.apache.hadoop.tools.posum.test.TestDataClientImpl;

/**
 * Created by ane on 8/1/16.
 */
public class TestMockDataStoreImpl extends TestDataClientImpl {

    @Override
    public void setUpDataStore() throws Exception {
        dataStore = new MockDataStoreImpl();
    }
}
