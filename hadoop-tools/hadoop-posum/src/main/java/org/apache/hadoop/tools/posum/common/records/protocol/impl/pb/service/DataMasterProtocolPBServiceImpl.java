package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.tools.posum.common.records.protocol.SingleObjectResponse;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.SingleObjectRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.SingleObjectResponsePBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleObjectResponseProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleObjectRequestProto;
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
    public SingleObjectResponseProto getObject(RpcController controller,
                                               SingleObjectRequestProto proto) throws ServiceException {
        SingleObjectRequestPBImpl request = new SingleObjectRequestPBImpl(proto);
        try {
            SingleObjectResponse response = real.getObject(request);
            return ((SingleObjectResponsePBImpl) response).getProto();
        } catch (YarnException | IOException e) {
            throw new ServiceException(e);
        }
    }
}
