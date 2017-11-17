package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.tools.posum.common.records.protocol.SimulatorMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.SimpleRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.response.impl.pb.SimpleResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.PosumProtos.SimpleRequestProto;
import org.apache.hadoop.yarn.proto.PosumProtos.SimpleResponseProto;

import java.io.IOException;

public class SimulatorMasterProtocolPBServiceImpl implements SimulatorMasterProtocolPB {

  private SimulatorMasterProtocol real;

  public SimulatorMasterProtocolPBServiceImpl(SimulatorMasterProtocol impl) {
    this.real = impl;
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
