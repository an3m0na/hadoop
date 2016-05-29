package org.apache.hadoop.tools.posum.database.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.field.MultiEntityPayload;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.field.SingleEntityPayload;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.common.util.StandardClientProxyFactory;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.request.MultiEntityRequest;
import org.apache.hadoop.tools.posum.common.records.field.EntityByIdPayload;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/9/16.
 */
public class DataMasterClient extends AbstractService {

    private static Log logger = LogFactory.getLog(DataMasterClient.class);

    private DataMasterProtocol dmClient;
    private String connectAddress;

    public DataMasterClient(String connectAddress) {
        super(DataMasterClient.class.getName());
        this.connectAddress = connectAddress;
    }

    public String getConnectAddress() {
        return connectAddress;
    }

    @Override
    protected void serviceStart() throws Exception {
        final Configuration conf = getConfig();
        try {
            dmClient = new StandardClientProxyFactory<>(conf,
                    connectAddress,
                    POSUMConfiguration.DM_ADDRESS_DEFAULT,
                    POSUMConfiguration.DM_PORT_DEFAULT,
                    DataMasterProtocol.class).createProxy();
            checkPing();
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

    public <T extends GeneralDataEntity> T findById(DataEntityDB db, DataEntityType collection, String id) {
        try {
            SingleEntityPayload payload = Utils.handleError("findById",
                    dmClient.getEntity(SimpleRequest.newInstance(SimpleRequest.Type.ENTITY_BY_ID,
                            EntityByIdPayload.newInstance(db, collection, id)))
            ).getPayload();
            if (payload != null)
                return (T) payload.getEntity();
            return null;
        } catch (IOException | YarnException e) {
            throw new POSUMException("Error during RPC call", e);
        }
    }

    public <T extends GeneralDataEntity> List<T> find(DataEntityDB db, DataEntityType collection, Map<String, Object> queryParams) {
        try {
            MultiEntityPayload payload = Utils.handleError("findById",
                    dmClient.listEntities(MultiEntityRequest.newInstance(db, collection, queryParams))).getPayload();
            if (payload != null)
                return (List<T>) payload.getEntities();
            return null;
        } catch (IOException | YarnException e) {
            throw new POSUMException("Error during RPC call", e);
        }
    }

    public JobProfile getJobProfileForApp(DataEntityDB db, String appId) {
        logger.debug("Getting job profile for app " + appId);
        try {
            SingleEntityPayload payload = Utils.handleError("getJobProfileForApp",
                    dmClient.getEntity(SimpleRequest.newInstance(SimpleRequest.Type.JOB_FOR_APP,
                            EntityByIdPayload.newInstance(db, null, appId)))
            ).getPayload();
            if (payload != null)
                return (JobProfile) payload.getEntity();
            return null;
        } catch (IOException | YarnException e) {
            throw new POSUMException("Error during RPC call", e);
        }
    }

    public <T extends GeneralDataEntity> String store(DataEntityDB db, DataEntityType collection, T toInsert) {
        //TODO
        return null;
    }

    public List<JobProfile> getComparableProfiles(DataEntityDB db, String user, int count) {
        //TODO
        return null;
    }

    public <T extends GeneralDataEntity> boolean updateOrStore(DataEntityDB db, DataEntityType apps, T toUpdate) {
        //TODO
        return false;
    }

    public void delete(DataEntityDB db, DataEntityType collection, String id) {
        //TODO
    }

    public void delete(DataEntityDB db, DataEntityType collection, Map<String, Object> queryParams) {
        //TODO
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

    private void checkPing() {
        sendSimpleRequest("checkPing", SimpleRequest.newInstance(SimpleRequest.Type.PING, "Hello world!"));
        logger.info("Successfully connected to Data Master");
    }

    public DBInterface bindTo(DataEntityDB db){
        return new DBImpl(db, this);
    }
}
