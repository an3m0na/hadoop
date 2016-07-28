package org.apache.hadoop.tools.posum.database.store;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.database.client.DataClientInterface;

import java.util.List;
import java.util.Map;


/**
 * Created by ane on 7/28/16.
 */
public interface DumpableDataClientInterface extends DataClientInterface{
    Map<DataEntityDB, List<DataEntityType>> listExistingCollections();
}
