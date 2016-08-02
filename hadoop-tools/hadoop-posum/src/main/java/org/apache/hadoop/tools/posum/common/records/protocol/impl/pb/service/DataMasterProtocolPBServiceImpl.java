package org.apache.hadoop.tools.posum.common.records.protocol.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.tools.posum.common.records.call.impl.pb.DatabaseCallWrapperPBImpl;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.response.impl.pb.*;
import org.apache.hadoop.tools.posum.common.records.request.impl.pb.SearchRequestPBImpl;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.proto.POSUMProtos;
import org.apache.hadoop.yarn.proto.POSUMProtos.SimpleResponseProto;
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
    public SimpleResponseProto executeDatabaseCall(RpcController controller,
                                                   POSUMProtos.DatabaseCallProto proto) throws ServiceException {
        try {
            SimpleResponse response = real.executeDatabaseCall(new DatabaseCallWrapperPBImpl(proto));
            return ((SimplePropertyResponsePBImpl) response).getProto();
        } catch (YarnException | IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public SimpleResponseProto getEntity(RpcController controller,
                                         POSUMProtos.SimpleRequestProto proto) throws ServiceException {
        try {
            SimpleResponse response = real.getEntity(Utils.wrapSimpleRequest(proto));
            return ((SingleEntityResponsePBImpl) response).getProto();
        } catch (YarnException | IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public SimpleResponseProto listEntities(RpcController controller,
                                            POSUMProtos.ByParamsProto proto) throws ServiceException {
        SearchRequestPBImpl request = new SearchRequestPBImpl(proto);
        try {
            SimpleResponse response = real.listEntities(request);
            return ((MultiEntityResponsePBImpl) response).getProto();
        } catch (YarnException | IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public SimpleResponseProto listIds(RpcController controller,
                                       POSUMProtos.ByParamsProto proto) throws ServiceException {
        SearchRequestPBImpl request = new SearchRequestPBImpl(proto);
        try {
            SimpleResponse response = real.listIds(request);
            return ((StringListResponsePBImpl) response).getProto();
        } catch (YarnException | IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public SimpleResponseProto handleSimpleRequest(RpcController controller, POSUMProtos.SimpleRequestProto request) throws ServiceException {
        try {
            SimpleResponse response = real.handleSimpleRequest(Utils.wrapSimpleRequest(request));
            return ((SimpleResponsePBImpl) response).getProto();
        } catch (YarnException | IOException e) {
            throw new ServiceException(e);
        }
    }
}
