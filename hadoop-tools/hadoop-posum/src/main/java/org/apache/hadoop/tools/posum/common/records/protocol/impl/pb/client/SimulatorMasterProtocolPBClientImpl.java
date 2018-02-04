package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.client;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.tools.posum.common.records.protocol.SimulatorMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service.SimulatorMasterProtocolPB;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.SimpleRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.util.communication.CommUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.PosumProtos.SimpleRequestProto;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

public class SimulatorMasterProtocolPBClientImpl implements SimulatorMasterProtocol, Closeable {

  private SimulatorMasterProtocolPB proxy;

  public SimulatorMasterProtocolPBClientImpl(long clientVersion, InetSocketAddress addr,
                                             Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, SimulatorMasterProtocolPB.class, ProtobufRpcEngine.class);
    proxy =
      (SimulatorMasterProtocolPB) RPC.getProxy(SimulatorMasterProtocolPB.class, clientVersion,
        addr, conf);
  }

  @Override
  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  @Override
  public SimpleResponse handleSimpleRequest(SimpleRequest request) throws IOException, YarnException {
    SimpleRequestProto requestProto =
      ((SimpleRequestPBImpl) request).getProto();
    try {
      return CommUtils.wrapSimpleResponse(proxy.handleSimpleRequest(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

}