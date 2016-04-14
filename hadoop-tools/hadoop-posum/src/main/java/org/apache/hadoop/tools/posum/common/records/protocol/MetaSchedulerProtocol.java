package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * Created by ane on 3/31/16.
 */
public interface MetaSchedulerProtocol {
    long versionID = 1L;

    SimpleResponse handleSimpleRequest(SimpleRequest request) throws IOException, YarnException;
}
