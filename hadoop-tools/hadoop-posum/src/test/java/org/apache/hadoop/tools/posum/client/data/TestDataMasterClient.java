package org.apache.hadoop.tools.posum.client.data;

import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.data.master.DataMaster;
import org.apache.hadoop.tools.posum.orchestration.master.OrchestrationMaster;
import org.apache.hadoop.tools.posum.test.IntegrationTest;
import org.apache.hadoop.tools.posum.test.ServiceRunner;
import org.apache.hadoop.tools.posum.test.Utils;
import org.junit.After;
import org.junit.experimental.categories.Category;


@Category(IntegrationTest.class)
public class TestDataMasterClient extends TestDataStore {
    private ServiceRunner posumMaster, dataMaster;
    private DataMasterClient client;

    @Override
    protected void setUpDataStore() throws Exception {
        System.out.println("Starting MongoDB and POSUM processes...");
        Utils.runMongoDB();
        posumMaster = new ServiceRunner<>(OrchestrationMaster.class);
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
}
