package org.apache.hadoop.tools.posum.database.store;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.util.json.JsonFileReader;
import org.apache.hadoop.tools.posum.database.client.DataClientInterface;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ane on 7/28/16.
 */
public class DataStoreImporter {
    private Map<DataEntityDB, Map<DataEntityType, File>> dataFiles = new HashMap<>(DataEntityDB.Type.values().length);

    public DataStoreImporter(String dumpPath) {
        File dumpDir = new File(dumpPath);
        try {
            File[] files = dumpDir.listFiles();
            Pattern p = Pattern.compile("^\\[(.*)\\](.*)\\.json$"); //[db]collection.json
            for (File file : files) {
                Matcher m = p.matcher(file.getName());
                if (m.find()) {
                    DataEntityDB db = DataEntityDB.fromName(m.group(1));
                    DataEntityType collection = DataEntityType.fromLabel(m.group(2));
                    if (db == null || collection == null)
                        continue;
                    Map<DataEntityType, File> dbFiles = dataFiles.get(db);
                    if (dbFiles == null) {
                        dbFiles = new HashMap<>();
                        dataFiles.put(db, dbFiles);
                    }
                    dbFiles.put(collection, file);
                }
            }
        } catch (Exception e) {
            throw new POSUMException("Data dump directory could not be read: " + dumpPath, e);
        }
    }

    public void importTo(DataClientInterface dataStore) {
        for (Map.Entry<DataEntityDB, Map<DataEntityType, File>> dbMapEntry : dataFiles.entrySet()) {
            for (Map.Entry<DataEntityType, File> fileEntry : dbMapEntry.getValue().entrySet()) {
                try {
                    JsonFileReader reader = new JsonFileReader(fileEntry.getValue());
                    Class<? extends GeneralDataEntity> entityClass = fileEntry.getKey().getMappedClass();
                    GeneralDataEntity entity;
                    while ((entity = reader.getNext(entityClass)) != null)
                        dataStore.updateOrStore(dbMapEntry.getKey(), fileEntry.getKey(), entity);
                    reader.close();
                } catch (IOException e) {
                    throw new POSUMException("Did not successfully parse file contents for " + fileEntry.getValue(), e);
                }
            }
        }
    }
}
