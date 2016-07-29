package org.apache.hadoop.tools.posum.common.records.request;


import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

/**
 * Created by ane on 3/20/16.
 */


public abstract class SearchRequest {

    public static SearchRequest newInstance(DataEntityDB db, DataEntityCollection type, Map<String, Object> properties, int offsetOrZero, int limitOrZero) {
        SearchRequest request = newInstance(db, type, properties);
        request.setLimitOrZero(limitOrZero);
        request.setOffsetOrZero(offsetOrZero);
        return request;
    }

    public static SearchRequest newInstance(DataEntityDB db, DataEntityCollection type, Map<String, Object> properties) {
        SearchRequest request = Records.newRecord(SearchRequest.class);
        request.setEntityDB(db);
        request.setEntityType(type);
        request.setProperties(properties);
        return request;
    }

    public abstract DataEntityDB getEntityDB();

    public abstract void setEntityDB(DataEntityDB db);

    public abstract DataEntityCollection getEntityType();

    public abstract void setEntityType(DataEntityCollection type);

    public abstract Map<String, Object> getProperties();

    public abstract void setProperties(Map<String, Object> properties);

    public abstract int getLimitOrZero();

    public abstract void setLimitOrZero(int limitOrZero);

    public abstract int getOffsetOrZero();

    public abstract void setOffsetOrZero(int offsetOrZero);
}
