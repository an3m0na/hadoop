package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.client;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.tools.posum.common.records.protocol.OrchestratorMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service.OrchestratorMasterProtocolPB;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.HandleSimResultRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.RegistrationRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.request.*;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.SimpleRequestPBImpl;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.PosumProtos.SimpleRequestProto;
import org.apache.hadoop.yarn.proto.PosumProtos.HandleSimResultRequestProto;
import org.apache.hadoop.yarn.proto.PosumProtos.RegistrationRequestProto;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by ane on 3/20/16.
 */
public class OrchestratorMasterProtocolPBClientImpl implements OrchestratorMasterProtocol, Closeable {

    private OrchestratorMasterProtocolPB proxy;

    public OrchestratorMasterProtocolPBClientImpl(long clientVersion, InetSocketAddress addr,
                                                  Configuration conf) throws IOException {
        RPC.setProtocolEngine(conf, OrchestratorMasterProtocolPB.class, ProtobufRpcEngine.class);
        proxy =
                (OrchestratorMasterProtocolPB) RPC.getProxy(OrchestratorMasterProtocolPB.class, clientVersion,
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
            return Utils.wrapSimpleResponse(proxy.handleSimpleRequest(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }

    @Override
    public SimpleResponse handleSimulationResult(HandleSimResultRequest request) throws IOException, YarnException {
        HandleSimResultRequestProto requestProto =
                ((HandleSimResultRequestPBImpl) request).getProto();
        try {
            return Utils.wrapSimpleResponse(proxy.handleSimulationResult(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }

    @Override
    public SimpleResponse registerProcess(RegistrationRequest request) throws IOException, YarnException {
        RegistrationRequestProto requestProto =
                ((RegistrationRequestPBImpl) request).getProto();
        try {
            return Utils.wrapSimpleResponse(proxy.registerProcess(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }

}