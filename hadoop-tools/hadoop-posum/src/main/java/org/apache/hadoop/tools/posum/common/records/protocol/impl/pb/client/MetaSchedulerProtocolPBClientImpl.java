package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.client;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.tools.posum.common.records.protocol.MetaSchedulerProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service.MetaSchedulerProtocolPB;
import org.apache.hadoop.tools.posum.common.records.reponse.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.reponse.impl.pb.SimpleResponsePBImpl;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.SimpleRequestPBImpl;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.POSUMProtos.SimpleRequestProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SimpleResponseProto;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by ane on 3/20/16.
 */
public class MetaSchedulerProtocolPBClientImpl implements MetaSchedulerProtocol, Closeable {

    private MetaSchedulerProtocolPB proxy;

    public MetaSchedulerProtocolPBClientImpl(long clientVersion, InetSocketAddress addr,
                                             Configuration conf) throws IOException {
        RPC.setProtocolEngine(conf, MetaSchedulerProtocolPB.class, ProtobufRpcEngine.class);
        proxy =
                (MetaSchedulerProtocolPB) RPC.getProxy(MetaSchedulerProtocolPB.class, clientVersion,
                        addr, conf);
    }

    @Override
    public void close() {
        if (this.proxy != null) {
            RPC.stopProxy(this.proxy);
        }
    }

    private static SimpleResponse wrapSimpleResponse(SimpleResponseProto proto) {
        try {
            Class<? extends SimpleResponsePBImpl> implClass =
                    SimpleResponse.Type.fromProto(proto.getType()).getImplClass();
            return implClass.getConstructor(SimpleResponseProto.class).newInstance(proto);
        } catch (Exception e) {
            throw new POSUMException("Could not construct response object", e);
        }
    }

    @Override
    public SimpleResponse handleSimpleRequest(SimpleRequest request) throws IOException, YarnException {
        SimpleRequestProto requestProto =
                ((SimpleRequestPBImpl) request).getProto();
        try {
            return wrapSimpleResponse(proxy.handleSimpleRequest(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }
}