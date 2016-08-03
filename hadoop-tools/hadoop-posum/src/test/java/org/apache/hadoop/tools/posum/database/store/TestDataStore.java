package org.apache.hadoop.tools.posum.database.store;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
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
        db = new DataStoreImpl(PosumConfiguration.newInstance()).bindTo(DataEntityDB.getMain());
    }

    @After
    public void tearDown() throws Exception {
        Utils.stopMongoDB();
    }
}
