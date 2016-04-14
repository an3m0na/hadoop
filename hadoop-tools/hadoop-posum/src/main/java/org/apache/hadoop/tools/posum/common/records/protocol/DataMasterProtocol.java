package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.request.MultiEntityRequest;
import org.apache.hadoop.tools.posum.common.records.response.MultiEntityPayload;
import org.apache.hadoop.tools.posum.common.records.request.SingleEntityRequest;
import org.apache.hadoop.tools.posum.common.records.response.SingleEntityPayload;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * Created by ane on 3/19/16.
 */
public interface DataMasterProtocol {
    long versionID = 1L;

    SimpleResponse<SingleEntityPayload> getEntity(SingleEntityRequest request) throws IOException, YarnException;
    SimpleResponse<MultiEntityPayload> listEntities(MultiEntityRequest request) throws IOException, YarnException;
}
