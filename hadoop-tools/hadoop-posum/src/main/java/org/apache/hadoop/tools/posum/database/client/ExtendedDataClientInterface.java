package org.apache.hadoop.tools.posum.database.client;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;

import java.util.List;
import java.util.Map;


/**
 * Created by ane on 7/28/16.
 */
public interface ExtendedDataClientInterface extends DataClientInterface {
    Map<DataEntityDB, List<DataEntityCollection>> listExistingCollections();

    void clear();

    void lockForRead(DataEntityDB db);

    void lockForWrite(DataEntityDB db);

    void unlockForRead(DataEntityDB db);

    void unlockForWrite(DataEntityDB db);

    void lockForWrite();

    void unlockForWrite();
}
