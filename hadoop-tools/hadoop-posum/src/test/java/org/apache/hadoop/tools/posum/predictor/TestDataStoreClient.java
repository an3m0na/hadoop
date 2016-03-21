package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.profile.AppProfile;
import org.apache.hadoop.tools.posum.database.client.DataStoreClient;
import org.apache.hadoop.tools.posum.database.store.DataCollection;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.tools.posum.database.store.DataStoreImpl;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by ane on 3/21/16.
 */
public class TestDataStoreClient {

    @Test
    public void checkOneObject() {
        Configuration conf = TestUtils.getConf();
        DataStoreClient dataStore = new DataStoreClient();
        dataStore.init(conf);
        dataStore.start();

        DataStore myStore = new DataStoreImpl(conf);
        String id = "blabla1";
        myStore.delete(DataCollection.APPS, id);
        AppProfile profile = new AppProfile(id);
        System.out.println(profile);
        profile.setStartTime(System.currentTimeMillis());
        profile.setFinishTime(System.currentTimeMillis() + 10000);
        myStore.updateOrStore(DataCollection.APPS, profile);

        AppProfile other = dataStore.findById(DataCollection.APPS, id);
        System.out.println(other);
        assertTrue(profile.getId().equals(other.getId()));
        myStore.delete(DataCollection.APPS, id);
    }
}
