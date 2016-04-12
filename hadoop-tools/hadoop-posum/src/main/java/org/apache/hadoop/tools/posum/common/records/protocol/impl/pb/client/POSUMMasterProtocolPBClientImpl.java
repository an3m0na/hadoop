package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.client;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.tools.posum.common.records.protocol.POSUMMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service.POSUMMasterProtocolPB;
import org.apache.hadoop.tools.posum.common.records.reponse.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.reponse.impl.pb.SimpleResponsePBImpl;
import org.apache.hadoop.tools.posum.common.records.request.*;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.HandleSchedulerEventRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.SchedulerAllocateRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.SimpleRequestPBImpl;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueInfoRequestPBImpl;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.POSUMProtos.SchedulerAllocateRequestProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.HandleSchedulerEventRequestProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SimpleResponseProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SimpleRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
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
    public SimpleResponse forwardToScheduler(SimpleRequest request) throws IOException, YarnException {
        SimpleRequestProto requestProto =
                ((SimpleRequestPBImpl) request).getProto();
        try {
            return wrapSimpleResponse(proxy.forwardToScheduler(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }

    @Override
    public SimpleResponse handleSchedulerEvent(HandleSchedulerEventRequest request) throws IOException, YarnException {
        HandleSchedulerEventRequestProto requestProto =
                ((HandleSchedulerEventRequestPBImpl) request).getProto();
        try {
            return wrapSimpleResponse(proxy.handleSchedulerEvent(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }

    @Override
    public SimpleResponse<Allocation> allocateResources(SchedulerAllocateRequest request) throws IOException, YarnException {
        SchedulerAllocateRequestProto requestProto =
                ((SchedulerAllocateRequestPBImpl) request).getProto();
        try {
            return wrapSimpleResponse(proxy.allocateResources(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }

    @Override
    public SimpleResponse<QueueInfo> getSchedulerQueueInfo(GetQueueInfoRequest request) throws IOException, YarnException {
        YarnServiceProtos.GetQueueInfoRequestProto requestProto =
                ((GetQueueInfoRequestPBImpl) request).getProto();
        try {
            return wrapSimpleResponse(proxy.getSchedulerQueueInfo(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }
}