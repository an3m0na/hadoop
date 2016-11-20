package org.apache.hadoop.tools.posum.database.util;

import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.json.JsonFileWriter;
import org.apache.hadoop.tools.posum.database.client.DataBroker;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 7/28/16.
 */
public class DataExporter {
    private Map<DataEntityDB, List<DataEntityCollection>> collections;
    private DataBroker dataBroker;

    public DataExporter(DataBroker dataBroker) {
        this.dataBroker = dataBroker;
        collections = dataBroker.listExistingCollections();
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
                        dataBroker.executeDatabaseCall(findAll, dbMapEntry.getKey()).getEntities();
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
