package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.client;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.tools.posum.common.records.protocol.POSUMMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service.POSUMMasterProtocolPB;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.request.*;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.SimpleRequestPBImpl;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.POSUMProtos.SimpleRequestProto;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by ane on 3/20/16.
 */
public class POSUMMasterProtocolPBClientImpl implements POSUMMasterProtocol, Closeable {

    private POSUMMasterProtocolPB proxy;

    public POSUMMasterProtocolPBClientImpl(long clientVersion, InetSocketAddress addr,
                                           Configuration conf) throws IOException {
        RPC.setProtocolEngine(conf, POSUMMasterProtocolPB.class, ProtobufRpcEngine.class);
        proxy =
                (POSUMMasterProtocolPB) RPC.getProxy(POSUMMasterProtocolPB.class, clientVersion,
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

}