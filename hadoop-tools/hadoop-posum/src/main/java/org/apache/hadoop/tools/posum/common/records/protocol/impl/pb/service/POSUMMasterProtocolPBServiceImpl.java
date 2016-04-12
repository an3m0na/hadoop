package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.HandleSchedulerEventRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.SchedulerAllocateRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.reponse.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.SimpleRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.reponse.impl.pb.SimpleResponsePBImpl;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueInfoRequestPBImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;

import java.io.IOException;

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
        try {
            SimpleResponse response = real.forwardToScheduler(request);
            return ((SimpleResponsePBImpl) response).getProto();
        } catch (YarnException | IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public POSUMProtos.SimpleResponseProto handleSchedulerEvent(RpcController controller,
                                                                POSUMProtos.HandleSchedulerEventRequestProto proto) throws ServiceException {
        HandleSchedulerEventRequestPBImpl request = new HandleSchedulerEventRequestPBImpl(proto);
        try {
            SimpleResponse response = real.handleSchedulerEvent(request);
            return ((SimpleResponsePBImpl) response).getProto();
        } catch (YarnException | IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public POSUMProtos.SimpleResponseProto allocateResources(RpcController controller,
                                                             POSUMProtos.SchedulerAllocateRequestProto proto) throws ServiceException {
        SchedulerAllocateRequestPBImpl request = new SchedulerAllocateRequestPBImpl(proto);
        try {
            SimpleResponse response = real.allocateResources(request);
            return ((SimpleResponsePBImpl) response).getProto();
        } catch (YarnException | IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public POSUMProtos.SimpleResponseProto getSchedulerQueueInfo(RpcController controller,
                                                                 YarnServiceProtos.GetQueueInfoRequestProto proto) throws ServiceException {
        GetQueueInfoRequestPBImpl request = new GetQueueInfoRequestPBImpl(proto);
        try {
            SimpleResponse response = real.getSchedulerQueueInfo(request);
            return ((SimpleResponsePBImpl) response).getProto();
        } catch (YarnException | IOException e) {
            throw new ServiceException(e);
        }
    }
}
