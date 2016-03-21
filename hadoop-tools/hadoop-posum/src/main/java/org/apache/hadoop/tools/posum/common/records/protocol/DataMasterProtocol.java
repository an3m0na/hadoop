package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * Created by ane on 3/19/16.
 */
public interface DataMasterProtocol {
    long versionID = 1L;

    SingleEntityResponse getObject(SingleEntityRequest request) throws IOException, YarnException;
}
