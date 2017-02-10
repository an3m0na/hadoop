package org.apache.hadoop.tools.posum.data.core;

import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.data.mock.data.MockDataStoreImpl;
import org.apache.hadoop.tools.posum.test.Utils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference.Type.MAIN;
import static org.apache.hadoop.tools.posum.common.util.Utils.ID_FIELD;
import static org.apache.hadoop.tools.posum.test.Utils.CLUSTER_TIMESTAMP;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDataImportExport {
  private Database db;
  private DataStore dataStore;

  @Before
  public void setUp() throws Exception {
    dataStore = new MockDataStoreImpl();
    db = Database.from(dataStore, DatabaseReference.get(MAIN, "unitTest"));
    Utils.loadThreeDefaultAppsAndJobs(db);
  }

  @After
  public void tearDown() throws Exception {
    dataStore.clearDatabase(db.getTarget());
  }

  @Test
  public void test() throws Exception {
    String dataDumpPath = Utils.TEST_TMP_DIR + File.separator + "importexport";
    new DataExporter(dataStore).exportTo(dataDumpPath);
    File tmpDir = new File(dataDumpPath);
    assertTrue(tmpDir.exists() && tmpDir.isDirectory());
    dataStore.clear();
    IdsByQueryCall listIds = IdsByQueryCall.newInstance(DataEntityCollection.APP, null, ID_FIELD, false);
    List<String> ids = db.executeDatabaseCall(listIds).getEntries();
    assertEquals(0, ids.size());
    new DataImporter(dataDumpPath).importTo(dataStore);
    ids = db.executeDatabaseCall(listIds).getEntries();
    assertEquals(3, ids.size());
    assertArrayEquals(new String[]{
      ApplicationId.newInstance(CLUSTER_TIMESTAMP, 1).toString(),
      ApplicationId.newInstance(CLUSTER_TIMESTAMP, 2).toString(),
      ApplicationId.newInstance(CLUSTER_TIMESTAMP, 3).toString()}, ids.toArray());
    for (File file : tmpDir.listFiles()) {
      assertTrue(file.delete());
    }
    assertTrue(tmpDir.delete());
  }
}
