package org.apache.hadoop.tools.posum.database.store;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.database.client.DataStoreClient;
import org.apache.hadoop.tools.posum.test.TestDataClientImpl;
import org.apache.hadoop.tools.posum.test.Utils;
import org.junit.After;

/**
 * Created by ane on 3/4/16.
 */
public class TestDataStore extends TestDataClientImpl {

    @Override
    protected void setUpDataStore() throws Exception {
        Utils.runMongoDB();
        dataBroker = new DataStoreClient(new DataStoreImpl(PosumConfiguration.newInstance()));
        dataBroker.bindTo(DataEntityDB.getMain());
    }

    @After
    public void tearDown() throws Exception {
        Utils.stopMongoDB();
    }
}
