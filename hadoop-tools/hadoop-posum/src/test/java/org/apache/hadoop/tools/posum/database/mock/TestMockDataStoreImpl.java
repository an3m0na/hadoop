package org.apache.hadoop.tools.posum.database.mock;

import org.apache.hadoop.tools.posum.test.TestDataBroker;

/**
 * Created by ane on 8/1/16.
 */
public class TestMockDataStoreImpl extends TestDataBroker {

    @Override
    public void setUpDataBroker() throws Exception {
        dataBroker = org.apache.hadoop.tools.posum.common.util.Utils.exposeDataStoreAsBroker(
                new MockDataStoreImpl()
        );
    }
}
