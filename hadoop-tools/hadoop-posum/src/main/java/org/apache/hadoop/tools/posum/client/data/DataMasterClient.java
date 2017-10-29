package org.apache.hadoop.tools.posum.client.data;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.payload.CollectionMapPayload;
import org.apache.hadoop.tools.posum.common.records.payload.DatabaseAlterationPayload;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.protocol.DataMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.request.DatabaseCallExecutionRequest;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.communication.StandardClientProxyFactory;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DataMasterClient extends AbstractService implements DataStore {

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

  public <T extends Payload> T execute(DatabaseCall<T> call, DatabaseReference db) {
    try {
      return (T) Utils.handleError("execute",
        dmClient.executeDatabaseCall(DatabaseCallExecutionRequest.newInstance(call, db))).getPayload();
    } catch (IOException | YarnException e) {
      throw new PosumException("Error during RPC call", e);
    }
  }

  @Override
  public Map<DatabaseReference, List<DataEntityCollection>> listCollections() {
    return Utils.<CollectionMapPayload>sendSimpleRequest(SimpleRequest.Type.LIST_COLLECTIONS, dmClient).getEntries();
  }

  @Override
  public void clear() {
    Utils.sendSimpleRequest(SimpleRequest.Type.CLEAR_DATA, dmClient);
  }

  @Override
  public void clearDatabase(DatabaseReference db) {
    Utils.sendSimpleRequest(
      "clearDatabase",
      SimpleRequest.newInstance(SimpleRequest.Type.CLEAR_DB, DatabaseAlterationPayload.newInstance(db)),
      dmClient
    );
  }

  @Override
  public void copyDatabase(DatabaseReference sourceDB, DatabaseReference destinationDB) {
    Utils.sendSimpleRequest(
      "copyDatabase",
      SimpleRequest.newInstance(SimpleRequest.Type.COPY_DB,
        DatabaseAlterationPayload.newInstance(sourceDB, destinationDB)),
      dmClient
    );
  }

  @Override
  public void copyCollections(DatabaseReference sourceDB, DatabaseReference destinationDB, List<DataEntityCollection> collections) {
    Utils.sendSimpleRequest(
      "copyCollection",
      SimpleRequest.newInstance(SimpleRequest.Type.COPY_COLL,
        DatabaseAlterationPayload.newInstance(sourceDB, destinationDB, collections)),
      dmClient
    );
  }

  @Override
  public void awaitUpdate(DatabaseReference db) throws InterruptedException {
    Utils.sendSimpleRequest(
      "awaitUpdate",
      SimpleRequest.newInstance(SimpleRequest.Type.AWAIT_UPDATE,
        DatabaseAlterationPayload.newInstance(db)),
      dmClient
    );
  }

  @Override
  public void notifyUpdate(DatabaseReference db) {
    Utils.sendSimpleRequest(
      "notifyUpdate",
      SimpleRequest.newInstance(SimpleRequest.Type.NOTIFY_UPDATE,
        DatabaseAlterationPayload.newInstance(db)),
      dmClient
    );
  }

  public void reset(){
    Utils.sendSimpleRequest(
      "reset",
      SimpleRequest.newInstance(SimpleRequest.Type.RESET),
      dmClient
    );
  }
}
