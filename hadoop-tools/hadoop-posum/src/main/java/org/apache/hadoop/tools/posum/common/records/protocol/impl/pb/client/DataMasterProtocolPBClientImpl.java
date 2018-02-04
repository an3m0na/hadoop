package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.client;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service.DataMasterProtocolPB;
import org.apache.hadoop.tools.posum.common.records.request.DatabaseCallExecutionRequest;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.DatabaseCallExecutionRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.SimpleRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.response.impl.pb.SimpleResponsePBImpl;
import org.apache.hadoop.tools.posum.common.util.communication.CommUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.PosumProtos.DatabaseCallExecutionRequestProto;
import org.apache.hadoop.yarn.proto.PosumProtos.SimpleRequestProto;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

public class DataMasterProtocolPBClientImpl implements DataMasterProtocol, Closeable {

  private DataMasterProtocolPB proxy;

  public DataMasterProtocolPBClientImpl(long clientVersion, InetSocketAddress addr,
                                        Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, DataMasterProtocolPB.class, ProtobufRpcEngine.class);
    proxy =
      (DataMasterProtocolPB) RPC.getProxy(DataMasterProtocolPB.class, clientVersion,
        addr, conf);
  }

  @Override
  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  @Override
  public SimpleResponse executeDatabaseCall(DatabaseCallExecutionRequest request) throws IOException, YarnException {
    DatabaseCallExecutionRequestProto callProto = ((DatabaseCallExecutionRequestPBImpl) request).getProto();
    try {
      return new SimpleResponsePBImpl(proxy.executeDatabaseCall(null, callProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
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