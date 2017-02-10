package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.tools.posum.common.records.request.DatabaseCallExecutionRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.util.StandardProtocol;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

public interface DataMasterProtocol extends StandardProtocol {
  SimpleResponse executeDatabaseCall(DatabaseCallExecutionRequest call) throws IOException, YarnException;
}
