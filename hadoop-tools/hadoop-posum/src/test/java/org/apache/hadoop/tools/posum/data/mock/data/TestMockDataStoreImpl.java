package org.apache.hadoop.tools.posum.data.mock.data;

import org.apache.hadoop.tools.posum.client.data.TestDataStore;

public class TestMockDataStoreImpl extends TestDataStore {

  @Override
  public void setUpDataStore() throws Exception {
    dataStore = new MockDataStoreImpl();
  }
}
