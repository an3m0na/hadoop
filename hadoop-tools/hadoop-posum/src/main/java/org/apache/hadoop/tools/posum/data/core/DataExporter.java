package org.apache.hadoop.tools.posum.data.core;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.json.JsonFileWriter;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DataExporter {
  private Map<DatabaseReference, List<DataEntityCollection>> collections;
  private DataStore dataStore;

  public DataExporter(DataStore dataStore) {
    this.dataStore = dataStore;
    collections = dataStore.listCollections();
  }

  public void exportTo(String dumpPath) throws IOException {
    File dumpDir = new File(dumpPath);
    FileUtils.forceMkdir(dumpDir);
    FindByQueryCall findAll = FindByQueryCall.newInstance(null, null);
    for (Map.Entry<DatabaseReference, List<DataEntityCollection>> dbMapEntry : collections.entrySet()) {
      for (DataEntityCollection collection : dbMapEntry.getValue()) {
        findAll.setEntityCollection(collection);
        List<GeneralDataEntity> entities =
          dataStore.execute(findAll, dbMapEntry.getKey()).getEntities();
        File outFile = new File(dumpDir,
          "[" + dbMapEntry.getKey().getName() + "]" + collection.getLabel() + ".json");

        try {
          JsonFileWriter writer = new JsonFileWriter(outFile);
          for (GeneralDataEntity entity : entities) {
            writer.write(entity);
          }
          writer.close();
        } catch (IOException e) {
          throw new PosumException("Did not successfully write file contents to " + outFile.getName(), e);
        }
      }
    }
  }
}
