package org.apache.hadoop.tools.posum.common.util.communication;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;

public class RestClient {

  private static Log logger = LogFactory.getLog(RestClient.class);

  private Client client;

  public RestClient() {
    ClientConfig clientConfig = new DefaultClientConfig();
    clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
    client = Client.create(clientConfig);
  }

  public enum TrackingUI {
    RM("ResourceManager", YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS, "ws/v1/cluster/"),
    HISTORY("History", JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_ADDRESS, "ws/v1/history/mapreduce/"),
    AM("ApplicationMaster", YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS, "proxy/%s/ws/v1/mapreduce/"),
    PS("PortfolioScheduler", getHostnameOnly(YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS) + ":" + PosumConfiguration.SCHEDULER_WEBAPP_PORT_DEFAULT, "/ajax/"),
    OM("OrchestrationMaster", "http://localhost:" + PosumConfiguration.MASTER_WEBAPP_PORT_DEFAULT, "/ajax/"),
    SM("SimulationMaster", "http://localhost:" + PosumConfiguration.SIMULATOR_WEBAPP_PORT_DEFAULT, "/ajax/"),
    DM("DataMaster", "http://localhost:" + PosumConfiguration.DM_WEBAPP_PORT_DEFAULT, "/ajax/");

    private static boolean updated = false;

    private static final Map<String, TrackingUI> labelMap = new HashMap<>();

    public String label;
    public String root;
    public String address;

    static {
      for (TrackingUI field : TrackingUI.values()) {
        labelMap.put(field.label, field);
      }
    }

    TrackingUI(String label, String address, String root) {
      this.label = label;
      this.address = address;
      this.root = root;
    }

    public static TrackingUI fromLabel(String label) {
      return labelMap.get(label);
    }

    public synchronized static boolean checkUpdated(Map<CommUtils.PosumProcess, String> addresses) {
      if (updated)
        return true;
      String psAddress = addresses.get(CommUtils.PosumProcess.PS);
      if (psAddress == null)
        return false;
      String pmAddress = addresses.get(CommUtils.PosumProcess.OM);
      if (pmAddress == null)
        return false;
      String dmAddress = addresses.get(CommUtils.PosumProcess.DM);
      if (dmAddress == null)
        return false;
      String smAddress = addresses.get(CommUtils.PosumProcess.SM);
      if (smAddress == null)
        return false;
      String rmAddress = getHostnameOnly(psAddress);
      PS.address = "http://" + psAddress;
      OM.address = "http://" + pmAddress;
      SM.address = "http://" + smAddress;
      DM.address = "http://" + dmAddress;
      //FIXME assuming that the scheduler, the AM proxy and the history server are all on the same node
      RM.address = "http://" + rmAddress + RM.address.substring(RM.address.indexOf(":"));
      HISTORY.address = "http://" + rmAddress + HISTORY.address.substring(HISTORY.address.indexOf(":"));
      AM.address = RM.address;
      updated = true;
      return true;
    }

    private static String getHostnameOnly(String address) {
      if (address.contains(":"))
        return address.substring(0, address.indexOf(":"));
      return address;
    }
  }

  public <T> T getInfo(Class<T> tClass, TrackingUI trackingUI, String path, String... args) {
    ClientResponse response;
    String destination = String.format(trackingUI.root + path, (Object[]) args);
    try {
      WebResource resource = client.resource(trackingUI.address).path(destination);
      response = resource.head();
      if (response.getStatus() != 200) {
        logger.debug("Could not connect to resource " + resource.toString());
        return null;
      }

      response = resource.accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);

      if (response.getStatus() != 200 || !response.getType().toString().contains(MediaType.APPLICATION_JSON)) {
        throw new PosumException("Error during request to server: " + resource + " " + response.getType());
      }

      try {
        T object = response.getEntity(tClass);
        logger.trace("[RestClient] Raw response:" + object);
        return object;
      } catch (Exception e) {
        logger.error("Could not parse response as JSON ", e);
      }
    } catch (Exception e) {
      logger.warn("Could not connect to url " + trackingUI.address + "/" + destination + ": " + e.getMessage());
      return null;
    }
    return null;
  }


}
