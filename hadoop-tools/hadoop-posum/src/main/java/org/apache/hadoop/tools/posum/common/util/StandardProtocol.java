package org.apache.hadoop.tools.posum.common.util;

import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

public interface StandardProtocol {
  long versionID = 1L;

  SimpleResponse handleSimpleRequest(SimpleRequest request) throws IOException, YarnException;
}
