package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.tools.posum.common.records.message.reponse.MultiEntityResponse;
import org.apache.hadoop.tools.posum.common.records.message.reponse.SingleEntityResponse;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.message.request.impl.pb.MultiEntityRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.message.reponse.impl.pb.MultiEntityResponsePBImpl;
import org.apache.hadoop.tools.posum.common.records.message.request.impl.pb.SingleEntityRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.message.reponse.impl.pb.SingleEntityResponsePBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleEntityRequestProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.MultiEntityRequestProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleEntityResponseProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.MultiEntityResponseProto;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * Created by ane on 3/20/16.
 */
public class DataMasterProtocolPBServiceImpl implements DataMasterProtocolPB {

    private DataMasterProtocol real;

    public DataMasterProtocolPBServiceImpl(DataMasterProtocol impl) {
        this.real = impl;
    }

    @Override
    public SingleEntityResponseProto getEntity(RpcController controller,
                                               SingleEntityRequestProto proto) throws ServiceException {
        SingleEntityRequestPBImpl request = new SingleEntityRequestPBImpl(proto);
        try {
            SingleEntityResponse response = real.getEntity(request);
            return ((SingleEntityResponsePBImpl) response).getProto();
        } catch (YarnException | IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public MultiEntityResponseProto listEntities(RpcController controller,
                                                 MultiEntityRequestProto proto) throws ServiceException {
        MultiEntityRequestPBImpl request = new MultiEntityRequestPBImpl(proto);
        try {
            MultiEntityResponse response = real.listEntities(request);
            return ((MultiEntityResponsePBImpl) response).getProto();
        } catch (YarnException | IOException e) {
            throw new ServiceException(e);
        }
    }
}
