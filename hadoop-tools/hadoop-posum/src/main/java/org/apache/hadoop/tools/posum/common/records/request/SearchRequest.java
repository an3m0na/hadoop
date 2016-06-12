package org.apache.hadoop.tools.posum.common.records.request;


import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

/**
 * Created by ane on 3/20/16.
 */


public abstract class SearchRequest {

    public static SearchRequest newInstance(DataEntityDB db, DataEntityType type, Map<String, Object> properties, int offset, int limit) {
        SearchRequest request = newInstance(db, type, properties);
        request.setLimit(limit);
        request.setOffset(offset);
        return request;
    }

    public static SearchRequest newInstance(DataEntityDB db, DataEntityType type, Map<String, Object> properties) {
        SearchRequest request = Records.newRecord(SearchRequest.class);
        request.setEntityDB(db);
        request.setEntityType(type);
        request.setProperties(properties);
        return request;
    }

    public abstract DataEntityDB getEntityDB();

    public abstract void setEntityDB(DataEntityDB db);

    public abstract DataEntityType getEntityType();

    public abstract void setEntityType(DataEntityType type);

    public abstract Map<String, Object> getProperties();

    public abstract void setProperties(Map<String, Object> properties);

    public abstract int getLimit();

    public abstract void setLimit(int limit);

    public abstract int getOffset();

    public abstract void setOffset(int offset);
}
