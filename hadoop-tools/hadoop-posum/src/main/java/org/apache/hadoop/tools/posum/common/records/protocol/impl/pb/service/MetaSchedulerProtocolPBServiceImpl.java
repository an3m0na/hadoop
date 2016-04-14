package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.reponse.impl.pb.SimpleResponsePBImpl;
import org.apache.hadoop.tools.posum.common.records.protocol.MetaSchedulerProtocol;
import org.apache.hadoop.tools.posum.common.records.reponse.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.SimpleRequestPBImpl;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.POSUMProtos.SimpleRequestProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SimpleResponseProto;

import java.io.IOException;

/**
 * Created by ane on 3/20/16.
 */
public class MetaSchedulerProtocolPBServiceImpl implements MetaSchedulerProtocolPB {

    private MetaSchedulerProtocol real;

    public MetaSchedulerProtocolPBServiceImpl(MetaSchedulerProtocol impl) {
        this.real = impl;
    }

    private static <T> SimpleRequest<T> wrapSimpleRequest(SimpleRequestProto proto) {
        try {
            Class<? extends SimpleRequestPBImpl> implClass =
                    SimpleRequest.Type.fromProto(proto.getType()).getImplClass();
            return implClass.getConstructor(SimpleRequestProto.class).newInstance(proto);
        } catch (Exception e) {
            throw new POSUMException("Could not construct request object for " + proto.getType(), e);
        }
    }
    @Override
    public SimpleResponseProto handleSimpleRequest(RpcController controller,
                                                         SimpleRequestProto request) throws ServiceException {
        try {
            SimpleResponse response = real.handleSimpleRequest(wrapSimpleRequest(request));
            return ((SimpleResponsePBImpl) response).getProto();
        } catch (YarnException | IOException e) {
            throw new ServiceException(e);
        }
    }

}
