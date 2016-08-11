package org.apache.hadoop.tools.posum.database.store;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.json.JsonFileWriter;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 7/28/16.
 */
public class DataStoreExporter {
    private Map<DataEntityDB, List<DataEntityCollection>> collections;
    private LockBasedDataStore dataStore;

    public DataStoreExporter(LockBasedDataStore dataStore) {
        this.dataStore = dataStore;
        collections = dataStore.listExistingCollections();
    }

    public void exportTo(String dumpPath) {
        File dumpDir = new File(dumpPath);
        if (!dumpDir.exists())
            if (!dumpDir.mkdirs())
                throw new PosumException("Could not create data dump directory: " + dumpPath);
        for (Map.Entry<DataEntityDB, List<DataEntityCollection>> dbMapEntry : collections.entrySet()) {
            for (DataEntityCollection collection : dbMapEntry.getValue()) {
                List<GeneralDataEntity> entities = dataStore.find(
                        dbMapEntry.getKey(),
                        collection,
                        Collections.<String, Object>emptyMap(),
                        null,
                        false,
                        0,
                        0
                );
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
