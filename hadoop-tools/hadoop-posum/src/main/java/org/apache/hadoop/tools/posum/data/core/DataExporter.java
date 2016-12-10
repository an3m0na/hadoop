package org.apache.hadoop.tools.posum.data.core;

import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.json.JsonFileWriter;
import org.apache.hadoop.tools.posum.client.data.DataStore;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DataExporter {
    private Map<DataEntityDB, List<DataEntityCollection>> collections;
    private DataStore dataStore;

    public DataExporter(DataStore dataStore) {
        this.dataStore = dataStore;
        collections = dataStore.listCollections();
    }

    public void exportTo(String dumpPath) {
        File dumpDir = new File(dumpPath);
        if (!dumpDir.exists())
            if (!dumpDir.mkdirs())
                throw new PosumException("Could not create data dump directory: " + dumpPath);
        FindByQueryCall findAll = FindByQueryCall.newInstance(null, null);
        for (Map.Entry<DataEntityDB, List<DataEntityCollection>> dbMapEntry : collections.entrySet()) {
            for (DataEntityCollection collection : dbMapEntry.getValue()) {
                findAll.setEntityCollection(collection);
                List<GeneralDataEntity> entities =
                        dataStore.executeDatabaseCall(findAll, dbMapEntry.getKey()).getEntities();
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
