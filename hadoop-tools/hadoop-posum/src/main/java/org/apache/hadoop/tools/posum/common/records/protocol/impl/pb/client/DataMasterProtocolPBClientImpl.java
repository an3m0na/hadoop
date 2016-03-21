package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.client;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.tools.posum.common.records.protocol.SingleObjectRequest;
import org.apache.hadoop.tools.posum.common.records.protocol.SingleObjectResponse;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service.DataMasterProtocolPB;
import org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.SingleObjectRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.SingleObjectResponsePBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleObjectRequestProto;
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
    public SingleObjectResponse getObject(SingleObjectRequest request) throws IOException, YarnException {
        SingleObjectRequestProto requestProto =
                ((SingleObjectRequestPBImpl) request).getProto();
        try {
            return new SingleObjectResponsePBImpl(
                    proxy.getObject(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }
}