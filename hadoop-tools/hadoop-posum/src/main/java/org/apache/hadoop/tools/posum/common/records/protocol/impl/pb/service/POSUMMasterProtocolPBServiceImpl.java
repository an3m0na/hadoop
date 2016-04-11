package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.tools.posum.common.records.message.simple.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.message.simple.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.message.impl.pb.*;
import org.apache.hadoop.tools.posum.common.records.message.simple.impl.pb.SimpleRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.message.simple.impl.pb.SimpleResponsePBImpl;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueInfoRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueInfoResponsePBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;

/**
 * Created by ane on 3/20/16.
 */
public class POSUMMasterProtocolPBServiceImpl implements POSUMMasterProtocolPB {

    private POSUMMasterProtocol real;

    public POSUMMasterProtocolPBServiceImpl(POSUMMasterProtocol impl) {
        this.real = impl;
    }

    @Override
    public POSUMProtos.SimpleResponseProto forwardToScheduler(RpcController controller, POSUMProtos.SimpleRequestProto proto) throws ServiceException {
        SimpleRequestPBImpl request;
        try {
            Class<? extends SimpleRequestPBImpl> implClass =
                    SimpleRequest.Type.fromProto(proto.getType()).getImplClass();
            request = implClass.getConstructor(POSUMProtos.SimpleRequestProto.class).newInstance(proto);
        } catch (Exception e) {
            throw new POSUMException("Could not construct request object for " + proto.getType(), e);
        }
        SimpleResponse response = real.forwardToScheduler(request);
        return ((SimpleResponsePBImpl) response).getProto();
    }

    @Override
    public POSUMProtos.SimpleResponseProto handleSchedulerEvent(RpcController controller,
                                                                POSUMProtos.HandleSchedulerEventRequestProto proto) throws ServiceException {
        HandleSchedulerEventRequestPBImpl request = new HandleSchedulerEventRequestPBImpl(proto);
        SimpleResponse response = real.handleSchedulerEvent(request);
        return ((SimpleResponsePBImpl) response).getProto();
    }

    @Override
    public POSUMProtos.SimpleResponseProto allocateResources(RpcController controller,
                                                                        POSUMProtos.SchedulerAllocateRequestProto proto) throws ServiceException {
        SchedulerAllocateRequestPBImpl request = new SchedulerAllocateRequestPBImpl(proto);
        SimpleResponse response = real.allocateResources(request);
        return ((SimpleResponsePBImpl) response).getProto();
    }

    @Override
    public YarnServiceProtos.GetQueueInfoResponseProto getSchedulerQueueInfo(RpcController
                                                                                     controller, YarnServiceProtos.GetQueueInfoRequestProto proto) throws ServiceException {
        GetQueueInfoRequestPBImpl request = new GetQueueInfoRequestPBImpl(proto);
        GetQueueInfoResponse response = real.getSchedulerQueueInfo(request);
        return ((GetQueueInfoResponsePBImpl) response).getProto();
    }
}
