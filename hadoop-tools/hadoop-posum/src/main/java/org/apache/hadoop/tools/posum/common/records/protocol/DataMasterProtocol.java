package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.payload.StringListPayload;
import org.apache.hadoop.tools.posum.common.records.request.SearchRequest;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.payload.MultiEntityPayload;
import org.apache.hadoop.tools.posum.common.records.payload.SingleEntityPayload;
import org.apache.hadoop.tools.posum.common.util.StandardProtocol;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * Created by ane on 3/19/16.
 */
public interface DataMasterProtocol extends StandardProtocol {
    SimpleResponse<? extends Payload> executeDatabaseCall(DatabaseCall call) throws IOException, YarnException;
    SimpleResponse<SingleEntityPayload> getEntity(SimpleRequest request) throws IOException, YarnException;
    SimpleResponse<MultiEntityPayload> listEntities(SearchRequest request) throws IOException, YarnException;
    SimpleResponse<StringListPayload> listIds(SearchRequest request) throws IOException, YarnException;
}
