package org.apache.hadoop.tools.posum.database.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.field.MultiEntityPayload;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.field.SingleEntityPayload;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.util.StandardClientProxyFactory;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.request.MultiEntityRequest;
import org.apache.hadoop.tools.posum.common.records.request.EntityByIdPayload;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.database.store.DataStoreInterface;
import org.apache.hadoop.tools.posum.database.store.DataTransaction;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/9/16.
 */
public class DataMasterClient extends AbstractService implements DataStoreInterface {

    public DataMasterClient() {
        super(DataMasterClient.class.getName());
    }

    private static Log logger = LogFactory.getLog(DataMasterClient.class);

    DataMasterProtocol dmClient;

    @Override
    protected void serviceStart() throws Exception {
        final Configuration conf = getConfig();
        try {
            dmClient = new StandardClientProxyFactory<>(conf, DataMasterProtocol.class).createProxy();
        } catch (IOException e) {
            throw new POSUMException("Could not init DataMaster client", e);
        }
        super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
        if (this.dmClient != null) {
            RPC.stopProxy(this.dmClient);
        }
        super.serviceStop();
    }

    @Override
    public <T extends GeneralDataEntity> T findById(DataEntityType collection, String id) {
        try {
            SingleEntityPayload payload = Utils.handleError("findById",
                    dmClient.getEntity(SimpleRequest.newInstance(SimpleRequest.Type.ENTITY_BY_ID,
                            EntityByIdPayload.newInstance(collection, id)))
            ).getPayload();
            if (payload != null)
                return (T) payload.getEntity();
            return null;
        } catch (IOException | YarnException e) {
            throw new POSUMException("Error during RPC call", e);
        }
    }

    @Override
    public <T extends GeneralDataEntity> List<T> find(DataEntityType collection, String field, Object value) {
        Map<String, Object> queryParams = new HashMap<>(1);
        queryParams.put(field, value);
        return find(collection, queryParams);
    }

    @Override
    public <T extends GeneralDataEntity> List<T> find(DataEntityType collection, Map<String, Object> queryParams) {
        try {
            MultiEntityPayload payload = Utils.handleError("findById",
                    dmClient.listEntities(MultiEntityRequest.newInstance(collection, queryParams))).getPayload();
            if (payload != null)
                return (List<T>) payload.getEntities();
            return null;
        } catch (IOException | YarnException e) {
            throw new POSUMException("Error during RPC call", e);
        }
    }

    @Override
    public <T extends GeneralDataEntity> List<T> list(DataEntityType collection) {
        return find(collection, new HashMap<String, Object>());
    }

    @Override
    public JobProfile getJobProfileForApp(String appId) {
//        try {
//            SingleEntityPayload payload = Utils.handleError("findById",
//                    dmClient.getEntity(EntityByIdPayload.newInstance(DataEntityType.JOB, id))
//            ).getPayload();
//            if (payload != null)
//                return (JobProfile) payload.getEntity();
        return null;
//        } catch (IOException | YarnException e) {
//            throw new POSUMException("Error during RPC call", e);
//        }
    }

    @Override
    public <T extends GeneralDataEntity> String store(DataEntityType collection, T toInsert) {
        return null;
    }

    @Override
    public List<JobProfile> getComparableProfiles(String user, int count) {
        return null;
    }

    @Override
    public void runTransaction(DataTransaction transaction) throws POSUMException {
        throw new POSUMException("Transactions are not supported remotely");
    }

    @Override
    public <T extends GeneralDataEntity> boolean updateOrStore(DataEntityType apps, T toUpdate) {
        return false;
    }

    @Override
    public void delete(DataEntityType collection, String id) {

    }

    @Override
    public void delete(DataEntityType collection, String field, Object value) {

    }

    @Override
    public void delete(DataEntityType collection, Map<String, Object> queryParams) {

    }

    public SimpleResponse sendSimpleRequest(SimpleRequest.Type type) {
        return sendSimpleRequest(type.name(), SimpleRequest.newInstance(type));
    }

    public SimpleResponse sendSimpleRequest(String kind, SimpleRequest request) {
        try {
            return Utils.handleError(kind, dmClient.handleSimpleRequest(request));
        } catch (IOException | YarnException e) {
            throw new POSUMException("Error during RPC call", e);
        }
    }

    public void checkPing() {
        sendSimpleRequest("checkPing", SimpleRequest.newInstance(SimpleRequest.Type.PING, "Hello world!"));
        logger.info("Successfully connected to Data Master");
    }
}
