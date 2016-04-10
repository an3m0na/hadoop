package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.client;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.tools.posum.common.records.message.MultiEntityRequest;
import org.apache.hadoop.tools.posum.common.records.message.MultiEntityResponse;
import org.apache.hadoop.tools.posum.common.records.message.SingleEntityRequest;
import org.apache.hadoop.tools.posum.common.records.message.SingleEntityResponse;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
import org.apache.hadoop.tools.posum.common.records.message.impl.pb.MultiEntityRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.message.impl.pb.MultiEntityResponsePBImpl;
import org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service.DataMasterProtocolPB;
import org.apache.hadoop.tools.posum.common.records.message.impl.pb.SingleEntityRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.message.impl.pb.SingleEntityResponsePBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleEntityRequestProto;
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
    public SingleEntityResponse getEntity(SingleEntityRequest request) throws IOException, YarnException {
        SingleEntityRequestProto requestProto =
                ((SingleEntityRequestPBImpl) request).getProto();
        try {
            return new SingleEntityResponsePBImpl(
                    proxy.getEntity(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }

    @Override
    public MultiEntityResponse listEntities(MultiEntityRequest request) throws IOException, YarnException {
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
}