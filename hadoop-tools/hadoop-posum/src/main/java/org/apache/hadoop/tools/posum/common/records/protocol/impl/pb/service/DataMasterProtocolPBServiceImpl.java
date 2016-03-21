package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.tools.posum.common.records.protocol.SingleEntityResponse;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.SingleEntityRequestPBImpl;
import org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.SingleEntityResponsePBImpl;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleEntityRequestProto;
import org.apache.hadoop.yarn.proto.POSUMProtos.SingleEntityResponseProto;
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
    public SingleEntityResponseProto getObject(RpcController controller,
                                               SingleEntityRequestProto proto) throws ServiceException {
        SingleEntityRequestPBImpl request = new SingleEntityRequestPBImpl(proto);
        try {
            SingleEntityResponse response = real.getObject(request);
            return ((SingleEntityResponsePBImpl) response).getProto();
        } catch (YarnException | IOException e) {
            throw new ServiceException(e);
        }
    }
}
