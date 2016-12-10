package org.apache.hadoop.tools.posum.data.client;

import org.apache.hadoop.tools.posum.client.data.DataMasterClient;
import org.apache.hadoop.tools.posum.common.records.call.*;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.orchestrator.master.OrchestratorMaster;
import org.apache.hadoop.tools.posum.data.master.DataMaster;
import org.apache.hadoop.tools.posum.test.IntegrationTest;
import org.apache.hadoop.tools.posum.test.ServiceRunner;
import org.apache.hadoop.tools.posum.client.data.TestDataStore;
import org.apache.hadoop.tools.posum.test.Utils;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


@Category(IntegrationTest.class)
public class TestDataMasterClient extends TestDataStore {
    private ServiceRunner posumMaster, dataMaster;
    private DataMasterClient client;

    @Override
    protected void setUpDataStore() throws Exception {
        System.out.println("Starting MongoDB and POSUM processes...");
        Utils.runMongoDB();
        posumMaster = new ServiceRunner<>(OrchestratorMaster.class);
        posumMaster.start();
        dataMaster = new ServiceRunner<>(DataMaster.class);
        dataMaster.start();
        System.out.println("Waiting for Data Master to be ready...");
        dataMaster.awaitAvailability();
        System.out.println("Data master ready.");
        client = new DataMasterClient(dataMaster.getService().getConnectAddress());
        client.init(PosumConfiguration.newInstance());
        client.start();
        dataStore = client;
        System.out.println("DB ready.");
    }

    @After
    public void tearDown() throws Exception {
        System.out.println("Stopping POSUM processes...");
        posumMaster.shutDown();
        dataMaster.shutDown();
        posumMaster.join();
        dataMaster.join();
        System.out.println("Processes stopped.");
        System.out.println("Stopping MongoDB processes...");
        Utils.stopMongoDB();
        System.out.println("MongoDB stopped.");
    }

    //    @Test
    public void testHistoryProfileManipulation() {
        //TODO refactor for test new structure
//        Configuration conf = POSUMConfiguration.newInstance();
//        DataMasterClient dataStore = new DataMasterClient(null);
//        dataStore.init(conf);
//        dataStore.start();
//        DataStore myStore = new DataStore(conf);
//
//        String appId = "testHistoryApp";
//        myStore.delete(mainDB, DataEntityCollection.HISTORY, Collections.singletonMap("originalId", (Object)appId));
//        AppProfile app = Records.newRecord(AppProfile.class);
//        app.setId(appId);
//        app.setStartTime(System.currentTimeMillis());
//        app.setFinishTime(System.currentTimeMillis() + 10000);
//        System.out.println(app);
//        HistoryProfile appHistory = new HistoryProfilePBImpl<>(DataEntityCollection.APP, app);
//        String historyId = myStore.store(mainDB, DataEntityCollection.HISTORY, appHistory);
//
//        Map<String, Object> properties = new HashMap<>();
//        properties.put("originalId", appId);
//        List<HistoryProfile> profilesById = dataStore.find(mainDB, DataEntityCollection.HISTORY, properties, 0, 0);
//        System.out.println(profilesById);
//        assertTrue(profilesById.size() == 1);
//        HistoryProfile otherHistory = profilesById.get(0);
//        assertEquals(appId, otherHistory.getOriginalId());
//        assertEquals(appHistory.getTimestamp(), otherHistory.getTimestamp());
//
//        myStore.delete(mainDB, DataEntityCollection.HISTORY, historyId);
    }

    @Test
    public void testLogging() throws Exception {
        String message = "Some message";
        StoreLogCall storeLog = StoreLogCall.newInstance(message);
        Long timestamp = storeLog.getLogEntry().getTimestamp();
        String logId = (String) client.executeDatabaseCall(storeLog, null).getValue();
        assertNotNull(logId);
        FindByIdCall getLog = FindByIdCall.newInstance(
                DataEntityCollection.AUDIT_LOG,
                logId
        );
        LogEntry<SimplePropertyPayload> log =
                client.executeDatabaseCall(getLog, DatabaseReference.getLogs()).getEntity();
        assertEquals(logId, log.getId());
        assertEquals(timestamp, log.getTimestamp());
        assertEquals(message, log.getDetails().getValue());
    }

    @Test
    public void testLogChronology() throws Exception {
        String first = "First", second = "Second";
        StoreLogCall storeLog = StoreLogCall.newInstance(first);
        Long firstTimestamp = storeLog.getLogEntry().getTimestamp();
        String firstId = (String) client.executeDatabaseCall(storeLog, null).getValue();
        assertNotNull(firstId);

        storeLog = StoreLogCall.newInstance(second);
        Long secondTimestamp = firstTimestamp + 1000;
        storeLog.getLogEntry().setTimestamp(secondTimestamp);
        String secondId = (String) client.executeDatabaseCall(storeLog, null).getValue();
        assertNotNull(secondId);

        FindByQueryCall getLog = FindByQueryCall.newInstance(
                DataEntityCollection.AUDIT_LOG,
                QueryUtils.and(
                        QueryUtils.is("type", LogEntry.Type.GENERAL),
                        QueryUtils.greaterThan("timestamp", firstTimestamp)
                )
        );
        List<LogEntry> logs =
                client.executeDatabaseCall(getLog, DatabaseReference.getLogs()).getEntities();
        assertEquals(1, logs.size());
        assertEquals(secondId, logs.get(0).getId());

        getLog = FindByQueryCall.newInstance(
                DataEntityCollection.AUDIT_LOG,
                QueryUtils.and(
                        QueryUtils.is("type", LogEntry.Type.GENERAL),
                        QueryUtils.greaterThanOrEqual("timestamp", firstTimestamp),
                        QueryUtils.lessThan("timestamp", secondTimestamp)
                )
        );
        logs = client.executeDatabaseCall(getLog, DatabaseReference.getLogs()).getEntities();
        assertEquals(1, logs.size());
        assertEquals(firstId, logs.get(0).getId());
    }
}
