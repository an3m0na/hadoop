package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.client;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.tools.posum.common.records.field.MultiEntityPayload;
import org.apache.hadoop.tools.posum.common.records.field.SingleEntityPayload;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.SimpleRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.response.impl.pb.MultiEntityResponsePBImpl;
import org.apache.hadoop.tools.posum.common.records.response.impl.pb.SingleEntityResponsePBImpl;
import org.apache.hadoop.tools.posum.common.records.request.MultiEntityRequest;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.MultiEntityRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service.DataMasterProtocolPB;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.POSUMProtos.SimpleRequestProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.MultiEntityRequestProto;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by ane on 3/20/16.
 */
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
    public SimpleResponse<SingleEntityPayload> getEntity(SimpleRequest request) throws IOException, YarnException {
        SimpleRequestProto requestProto =
                ((SimpleRequestPBImpl) request).getProto();
        try {
            return new SingleEntityResponsePBImpl(
                    proxy.getEntity(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }

    @Override
    public SimpleResponse<MultiEntityPayload> listEntities(MultiEntityRequest request) throws IOException, YarnException {
        MultiEntityRequestProto requestProto =
                ((MultiEntityRequestPBImpl) request).getProto();
        try {
            return new MultiEntityResponsePBImpl(
                    proxy.listEntities(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }

    @Override
    public SimpleResponse handleSimpleRequest(SimpleRequest request) throws IOException, YarnException {
        POSUMProtos.SimpleRequestProto requestProto =
                ((SimpleRequestPBImpl) request).getProto();
        try {
            return Utils.wrapSimpleResponse(proxy.handleSimpleRequest(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }
}