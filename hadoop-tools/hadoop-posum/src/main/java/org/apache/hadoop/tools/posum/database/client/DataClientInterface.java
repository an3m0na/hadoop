package org.apache.hadoop.tools.posum.database.client;

import org.apache.hadoop.tools.posum.common.records.dataentity.*;

import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/9/16.
 */
public interface DataClientInterface {

    <T extends GeneralDataEntity> T findById(DataEntityDB db, DataEntityType collection, String id);

    List<String> listIds(DataEntityDB db, DataEntityType collection, Map<String, Object> queryParams);

    <T extends GeneralDataEntity> List<T> find(DataEntityDB db, DataEntityType collection, Map<String, Object> queryParams, int offset, int limit);

    <T extends GeneralDataEntity> String store(DataEntityDB db, DataEntityType collection, T toInsert);

    <T extends GeneralDataEntity> boolean updateOrStore(DataEntityDB db, DataEntityType apps, T toUpdate);

    void delete(DataEntityDB db, DataEntityType collection, String id);

    void delete(DataEntityDB db, DataEntityType collection, Map<String, Object> queryParams);

    DBInterface bindTo(DataEntityDB db);

    JobProfile getJobProfileForApp(DataEntityDB db, String appId, String user);

    void saveFlexFields(DataEntityDB db, String jobId, Map<String, String> newFields, boolean forHistory);
}
