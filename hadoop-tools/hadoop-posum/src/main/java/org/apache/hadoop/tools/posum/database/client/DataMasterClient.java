package org.apache.hadoop.tools.posum.database.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.*;
import org.apache.hadoop.tools.posum.common.records.payload.*;
import org.apache.hadoop.tools.posum.common.records.request.DatabaseCallExecutionRequest;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.StandardClientProxyFactory;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/9/16.
 */
public class DataMasterClient extends AbstractService implements DataBroker {

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
                    PosumConfiguration.DM_ADDRESS_DEFAULT,
                    PosumConfiguration.DM_PORT_DEFAULT,
                    DataMasterProtocol.class).createProxy();
            Utils.checkPing(dmClient);
            logger.info("Successfully connected to Data Master");
        } catch (IOException e) {
            throw new PosumException("Could not init DataMaster client", e);
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

    public <T extends Payload> T executeDatabaseCall(DatabaseCall<T> call, DataEntityDB db) {
        try {
            return (T) Utils.handleError("executeDatabaseCall",
                    dmClient.executeDatabaseCall(DatabaseCallExecutionRequest.newInstance(call, db))).getPayload();
        } catch (IOException | YarnException e) {
            throw new PosumException("Error during RPC call", e);
        }
    }

    @Override
    public Map<DataEntityDB, List<DataEntityCollection>> listExistingCollections() {
        return Utils.<CollectionMapPayload>sendSimpleRequest(SimpleRequest.Type.LIST_COLLECTIONS, dmClient).getEntries();
    }

    @Override
    public void clear() {
        Utils.sendSimpleRequest(SimpleRequest.Type.CLEAR_DATA, dmClient);
    }

    @Override
    public Database bindTo(DataEntityDB db) {
        return new DatabaseImpl(this, db);
    }

}
