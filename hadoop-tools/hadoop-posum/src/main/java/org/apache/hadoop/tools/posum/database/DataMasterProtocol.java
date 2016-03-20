package org.apache.hadoop.tools.posum.database;

import org.apache.hadoop.tools.posum.common.records.protocol.ListObjectsRequest;
import org.apache.hadoop.tools.posum.common.records.protocol.ListObjectsResponse;
import org.apache.hadoop.tools.posum.common.records.protocol.SingleObjectRequest;
import org.apache.hadoop.tools.posum.common.records.protocol.SingleObjectResponse;

/**
 * Created by ane on 3/19/16.
 */
public interface DataMasterProtocol {

    ListObjectsResponse listObjects(ListObjectsRequest request);
    SingleObjectResponse getObject(SingleObjectRequest request);
}
