package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.tools.posum.common.records.AppProfile;
import org.apache.hadoop.tools.posum.database.DataCollection;
import org.apache.hadoop.tools.posum.database.DataStore;
import org.apache.hadoop.tools.posum.database.DataStoreImpl;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by ane on 3/4/16.
 */
public class TestDataStoreImpl {
    @Test
    public void checkDatabase() {
        DataStore dataStore = new DataStoreImpl(TestUtils.getConf());
        dataStore.delete(DataCollection.APPS, "blabla1");
        dataStore.delete(DataCollection.APPS, "blabla2");
        AppProfile profile = new AppProfile("blabla1");
        dataStore.store(DataCollection.APPS, profile);
        profile.setFinishTime(System.currentTimeMillis());
        dataStore.updateOrStore(DataCollection.APPS, profile);
        profile.setId("blabla2");
        assertTrue(dataStore.updateOrStore(DataCollection.APPS, profile));
    }
}
