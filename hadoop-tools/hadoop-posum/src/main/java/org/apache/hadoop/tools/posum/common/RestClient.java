package org.apache.hadoop.tools.posum.common;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;

import javax.ws.rs.WebApplicationException;

/**
 * Created by ane on 2/24/16.
 */
public class RestClient {

    WebResource target;

    public RestClient() {

        ClientConfig clientConfig = new DefaultClientConfig();
        clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        Client client = Client.create(clientConfig);
         target = client.resource("http://localhost:8088").path("ws/v1");


    }

    public String testClient(){
        WebResource webResourceGet = target.path("cluster/info");//.queryParam("id", "1");
        ClientResponse response = webResourceGet.get(ClientResponse.class);
        ClusterInfo responseEntity = response.getEntity(ClusterInfo.class);

        if (response.getStatus() != 200) {
            throw new WebApplicationException();
        }

        return responseEntity.getHadoopVersion();
    }
}
