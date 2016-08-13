package org.apache.hadoop.tools.posum.database.client;

import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.FindByParamsCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreLogCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityFactory;
import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.orchestrator.master.OrchestratorMaster;
import org.apache.hadoop.tools.posum.database.master.DataMaster;
import org.apache.hadoop.tools.posum.test.ServiceRunner;
import org.apache.hadoop.tools.posum.test.TestDataBroker;
import org.apache.hadoop.tools.posum.test.Utils;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by ane on 3/21/16.
 */
public class TestDataMasterClient extends TestDataBroker {
    private ServiceRunner posumMaster, dataMaster;
    private DataMasterClient client;

    @Override
    protected void setUpDataBroker() throws Exception {
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
        dataBroker = client;
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
                client.executeDatabaseCall(getLog, DataEntityDB.getLogs()).getEntity();
        assertEquals(logId, log.getId());
        assertEquals(timestamp, log.getTimestamp());
        assertEquals(message, log.getDetails().getValue());
    }
}
