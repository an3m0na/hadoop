package org.apache.hadoop.tools.posum.database.client;

import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.core.orchestrator.OrchestratorMaster;
import org.apache.hadoop.tools.posum.database.master.DataMaster;
import org.apache.hadoop.tools.posum.test.ServiceRunner;
import org.apache.hadoop.tools.posum.test.TestDataClientImpl;
import org.apache.hadoop.tools.posum.test.Utils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Test;

/**
 * Created by ane on 3/21/16.
 */
public class TestDataMasterClient extends TestDataClientImpl {
    private ServiceRunner posumMaster, dataMaster;
    private DataMasterClient client;

    @Override
    protected void setUpDataStore() throws Exception {
        Utils.runMongoDB();
        posumMaster = new ServiceRunner<>(OrchestratorMaster.class);
        posumMaster.start();
        dataMaster = new ServiceRunner<>(DataMaster.class);
        dataMaster.start();
        dataMaster.awaitAvailability();
        client = new DataMasterClient(dataMaster.getService().getConnectAddress());
        client.init(PosumConfiguration.newInstance());
        client.start();
        dataStore = client;
    }

    @After
    public void tearDown() throws Exception {
        posumMaster.shutDown();
        dataMaster.shutDown();
        posumMaster.join();
        dataMaster.join();
        Utils.stopMongoDB();
    }

    @Test
    public void testRefactoring() throws Exception {
        AppProfile app = Records.newRecord(AppProfile.class);
        app.setId(ApplicationId.newInstance(clusterTimestamp, 1).toString());
        SimplePropertyPayload payload = client.executeDatabaseCall(
                StoreCall.newInstance(DataEntityDB.getMain(), DataEntityCollection.APP, app));
        System.out.println(payload.getValue());
    }

    @Test
    public void testHistoryProfileManipulation() {
        //TODO refactor for test new structure
//        Configuration conf = POSUMConfiguration.newInstance();
//        DataMasterClient dataStore = new DataMasterClient(null);
//        dataStore.init(conf);
//        dataStore.start();
//        DataStore myStore = new DataStore(conf);
//
//        String appId = "testHistoryApp";
//        myStore.delete(db, DataEntityCollection.HISTORY, Collections.singletonMap("originalId", (Object)appId));
//        AppProfile app = Records.newRecord(AppProfile.class);
//        app.setId(appId);
//        app.setStartTime(System.currentTimeMillis());
//        app.setFinishTime(System.currentTimeMillis() + 10000);
//        System.out.println(app);
//        HistoryProfile appHistory = new HistoryProfilePBImpl<>(DataEntityCollection.APP, app);
//        String historyId = myStore.store(db, DataEntityCollection.HISTORY, appHistory);
//
//        Map<String, Object> properties = new HashMap<>();
//        properties.put("originalId", appId);
//        List<HistoryProfile> profilesById = dataStore.find(db, DataEntityCollection.HISTORY, properties, 0, 0);
//        System.out.println(profilesById);
//        assertTrue(profilesById.size() == 1);
//        HistoryProfile otherHistory = profilesById.get(0);
//        assertEquals(appId, otherHistory.getOriginalId());
//        assertEquals(appHistory.getTimestamp(), otherHistory.getTimestamp());
//
//        myStore.delete(db, DataEntityCollection.HISTORY, historyId);
    }
}
