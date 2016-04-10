package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.tools.posum.common.records.message.MultiEntityRequest;
import org.apache.hadoop.tools.posum.common.records.message.MultiEntityResponse;
import org.apache.hadoop.tools.posum.common.records.message.SingleEntityRequest;
import org.apache.hadoop.tools.posum.common.records.message.SingleEntityResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * Created by ane on 3/19/16.
 */
public interface DataMasterProtocol {
    long versionID = 1L;

    SingleEntityResponse getEntity(SingleEntityRequest request) throws IOException, YarnException;
    MultiEntityResponse listEntities(MultiEntityRequest request) throws IOException, YarnException;
}
