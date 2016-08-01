package org.apache.hadoop.tools.posum.database.mock;

import org.apache.hadoop.tools.posum.test.TestDataClientImplementations;

/**
 * Created by ane on 8/1/16.
 */
public class TestMockDataStoreImpl extends TestDataClientImplementations {

    @Override
    public void setUpDataStore() throws Exception {
        dataStore = new MockDataStoreImpl();
    }
}
