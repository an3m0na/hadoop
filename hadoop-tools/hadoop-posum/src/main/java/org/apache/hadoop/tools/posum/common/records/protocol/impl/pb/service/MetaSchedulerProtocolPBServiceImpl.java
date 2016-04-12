package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.HandleRMEventRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.reponse.impl.pb.SimpleResponsePBImpl;
import org.apache.hadoop.tools.posum.common.records.protocol.MetaSchedulerProtocol;
import org.apache.hadoop.tools.posum.common.records.reponse.SimpleResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.POSUMProtos;

import java.io.IOException;

/**
 * Created by ane on 3/20/16.
 */
public class MetaSchedulerProtocolPBServiceImpl implements MetaSchedulerProtocolPB {

    private MetaSchedulerProtocol real;

    public MetaSchedulerProtocolPBServiceImpl(MetaSchedulerProtocol impl) {
        this.real = impl;
    }

    @Override
    public POSUMProtos.SimpleResponseProto handleRMEvent(RpcController controller,
                                                         POSUMProtos.HandleRMEventRequestProto proto) throws ServiceException {
        HandleRMEventRequestPBImpl request = new HandleRMEventRequestPBImpl(proto);
        try {
            SimpleResponse response = real.handleRMEvent(request);
            return ((SimpleResponsePBImpl) response).getProto();
        } catch (YarnException | IOException e) {
            throw new ServiceException(e);
        }
    }

}
