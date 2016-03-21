package org.apache.hadoop.tools.posum.database.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.POSUMException;
import org.apache.hadoop.tools.posum.common.StandardClientProxyFactory;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.protocol.SingleEntityRequest;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/9/16.
 */
public class DataStoreClient extends AbstractService implements DataStore {

    public DataStoreClient() {
        super(DataStoreClient.class.getName());
    }

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
            return (T)dmClient.getObject(SingleEntityRequest.newInstance(collection, id)).getEntity();
        } catch (IOException | YarnException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public JobProfile getJobProfileForApp(String appId) {
        return null;
    }

    @Override
    public <T extends GeneralDataEntity> void store(DataEntityType collection, T toInsert) {

    }

    @Override
    public List<JobProfile> getComparableProfiles(String user, int count) {
        return null;
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
}
