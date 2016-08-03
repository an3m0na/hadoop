package org.apache.hadoop.tools.posum.database.store;

import org.apache.hadoop.tools.posum.common.records.call.IdsByParamsCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.database.client.DataStoreClient;
import org.apache.hadoop.tools.posum.database.client.DataBroker;
import org.apache.hadoop.tools.posum.database.mock.MockDataStoreImpl;
import org.apache.hadoop.tools.posum.test.Utils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by ane on 7/29/16.
 */
public class TestDataImportExport {
    private DataBroker dataBroker;
    private final Long clusterTimestamp = System.currentTimeMillis();

    @Before
    public void setUp() throws Exception {
        dataBroker = new DataStoreClient(new MockDataStoreImpl());
        dataBroker.bindTo(DataEntityDB.getMain());
        Utils.loadThreeDefaultAppsAndJobs(clusterTimestamp, dataBroker);
    }

    @Test
    public void test() throws Exception {
        String dataDumpPath = "testTmpDir";
        File tmpDir = new File(dataDumpPath);
        assertTrue(tmpDir.exists() && tmpDir.isDirectory());
        dataBroker.clear();
        IdsByParamsCall listIds = IdsByParamsCall.newInstance(DataEntityCollection.APP, Collections.<String, Object>emptyMap());
        List<String> ids = dataBroker.executeDatabaseCall(listIds).getEntries();
        assertEquals(0, ids.size());
        ids = dataBroker.executeDatabaseCall(listIds).getEntries();
        assertEquals(3, ids.size());
        assertArrayEquals(new String[]{
                ApplicationId.newInstance(clusterTimestamp, 1).toString(),
                ApplicationId.newInstance(clusterTimestamp, 2).toString(),
                ApplicationId.newInstance(clusterTimestamp, 3).toString()}, ids.toArray());
        for (File file : tmpDir.listFiles()) {
            assertTrue(file.delete());
        }
        assertTrue(tmpDir.delete());
    }
}
