package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.DatabaseCallExecutionRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.SimpleRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.response.impl.pb.SimpleResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.PosumProtos.DatabaseCallExecutionRequestProto;
import org.apache.hadoop.yarn.proto.PosumProtos.SimpleRequestProto;
import org.apache.hadoop.yarn.proto.PosumProtos.SimpleResponseProto;

import java.io.IOException;

public class DataMasterProtocolPBServiceImpl implements DataMasterProtocolPB {

  private DataMasterProtocol real;

  public DataMasterProtocolPBServiceImpl(DataMasterProtocol impl) {
    this.real = impl;
  }

  @Override
  public SimpleResponseProto executeDatabaseCall(RpcController controller,
                                                 DatabaseCallExecutionRequestProto proto) throws ServiceException {
    try {
      SimpleResponse response = real.executeDatabaseCall(new DatabaseCallExecutionRequestPBImpl(proto));
      return ((SimpleResponsePBImpl) response).getProto();
    } catch (YarnException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SimpleResponseProto handleSimpleRequest(RpcController controller, SimpleRequestProto request) throws ServiceException {
    try {
      SimpleResponse response = real.handleSimpleRequest(new SimpleRequestPBImpl(request));
      return ((SimpleResponsePBImpl) response).getProto();
    } catch (YarnException | IOException e) {
      throw new ServiceException(e);
    }
  }
}
