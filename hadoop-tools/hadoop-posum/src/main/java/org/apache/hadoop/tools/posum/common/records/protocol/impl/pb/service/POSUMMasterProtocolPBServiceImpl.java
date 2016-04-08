package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.tools.posum.common.records.protocol.*;
import org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.*;
import org.apache.hadoop.yarn.proto.POSUMProtos;

/**
 * Created by ane on 3/20/16.
 */
public class POSUMMasterProtocolPBServiceImpl implements POSUMMasterProtocolPB {

    private POSUMMasterProtocol real;

    public POSUMMasterProtocolPBServiceImpl(POSUMMasterProtocol impl) {
        this.real = impl;
    }

    @Override
    public POSUMProtos.SimpleResponseProto configureScheduler(RpcController controller,
                                                              POSUMProtos.ConfigurationRequestProto proto) throws ServiceException {
        ConfigurationRequestPBImpl request = new ConfigurationRequestPBImpl(proto);
        SimpleResponse response = real.configureScheduler(request);
        return ((SimpleResponsePBImpl) response).getProto();
    }

    @Override
    public POSUMProtos.SimpleResponseProto initScheduler(RpcController controller,
                                                         POSUMProtos.ConfigurationRequestProto proto) throws ServiceException {
        ConfigurationRequestPBImpl request = new ConfigurationRequestPBImpl(proto);
        SimpleResponse response = real.initScheduler(request);
        return ((SimpleResponsePBImpl) response).getProto();
    }

    @Override
    public POSUMProtos.SimpleResponseProto reinitScheduler(RpcController controller,
                                                           POSUMProtos.ConfigurationRequestProto proto) throws ServiceException {
        ConfigurationRequestPBImpl request = new ConfigurationRequestPBImpl(proto);
        SimpleResponse response = real.reinitScheduler(request);
        return ((SimpleResponsePBImpl) response).getProto();
    }

    @Override
    public POSUMProtos.HandleEventResponseProto handleSchedulerEvent(RpcController controller,
                                                           POSUMProtos.HandleEventRequestProto proto) throws ServiceException {
        HandleEventRequestPBImpl request = new HandleEventRequestPBImpl(proto);
        HandleEventResponse response = real.handleSchedulerEvent(request);
        return ((HandleEventResponsePBImpl) response).getProto();
    }
}
