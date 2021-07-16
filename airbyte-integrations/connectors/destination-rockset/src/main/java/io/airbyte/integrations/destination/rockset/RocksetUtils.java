package io.airbyte.integrations.destination.rockset;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.internal.LinkedTreeMap;
import com.rockset.client.ApiClient;
import com.rockset.client.ApiException;
import com.rockset.client.RocksetClient;
import com.rockset.client.model.Collection;
import com.rockset.client.model.CreateCollectionRequest;
import com.rockset.client.model.CreateCollectionResponse;
import com.rockset.client.model.CreateWorkspaceRequest;
import com.rockset.client.model.CreateWorkspaceResponse;

import com.rockset.client.model.DeleteDocumentsRequest;
import com.rockset.client.model.DeleteDocumentsRequestData;
import com.rockset.client.model.DeleteDocumentsResponse;
import com.rockset.client.model.ErrorModel;
import com.rockset.client.model.GetCollectionResponse;
import com.rockset.client.model.QueryRequest;
import com.rockset.client.model.QueryRequestSql;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

public class RocksetUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(RocksetUtils.class);

  public static final String WORKSPACE_ID = "workspace";
  public static final String API_KEY_ID = "api_key";
  public static final String APISERVER_URL = "api.rs2.usw2.rockset.com";

  public static RocksetClient clientFromConfig(JsonNode config) {
    String apiKey = config.get(API_KEY_ID).asText();
    return new RocksetClient(apiKey, APISERVER_URL);
  }

  public static ApiClient apiClientFromConfig(JsonNode config) {
    String apiKey = config.get(API_KEY_ID).asText();
    ApiClient client = new ApiClient();
    client.setApiKey(apiKey);
    client.setVersion("0.9.0");
    return client;
  }

  public static void createWorkspaceIfNotExists(RocksetClient client, String workspace)
      throws Exception {
    CreateWorkspaceRequest request = new CreateWorkspaceRequest().name(workspace);

    try {
      client.createWorkspace(request);
      LOGGER.info(String.format("Created workspace %s", workspace));
    } catch (ApiException e) {
      if (e.getCode() == 400 && e.getErrorModel().getType() == ErrorModel.TypeEnum.ALREADYEXISTS) {
        LOGGER.info(String.format("Workspace %s already exists", workspace));
        return;
      }

      throw e;
    }
  }

  // Assumes the workspace exists
  public static void createCollectionIfNotExists(
      RocksetClient client, String workspace, String cname) throws Exception {
    CreateCollectionRequest request = new CreateCollectionRequest().name(cname);
    try {
      client.createCollection(workspace, request);
      LOGGER.info(String.format("Created collection %s.%s", workspace, cname));
    } catch (ApiException e) {
      if (e.getCode() == 400 && e.getErrorModel().getType() == ErrorModel.TypeEnum.ALREADYEXISTS) {
        LOGGER.info(String.format("Collection %s.%s already exists", workspace, cname));
        return;
      }

      throw e;
    }
  }

  // Assumes the collection exists
  public static void waitUntilCollectionReady(RocksetClient client, String workspace, String cname)
      throws Exception {
    while (true) {
      GetCollectionResponse resp = client.getCollection(workspace, cname);
      Collection.StatusEnum status = resp.getData().getStatus();
      if (status == Collection.StatusEnum.READY) {
        LOGGER.info(String.format("Collection %s.%s is READY", workspace, cname));
        break;
      } else {
        LOGGER.info(
            String.format(
                "Waiting until %s.%s is READY, it is %s", workspace, cname, status.toString()));
        Thread.sleep(5000);
      }
    }
  }

  public static void deleteAllDocsInCollection(RocksetClient client, String workspace, String cname)
      throws Exception {
    List<DeleteDocumentsRequestData> allDocIds =
        client
            .query(
                new QueryRequest()
                    .sql(
                        new QueryRequestSql()
                            // FIX, unescaped params
                            .query(String.format("select _id from %s.%s", workspace, cname))))
            .getResults()
            .stream()
            .map(x -> (LinkedTreeMap<String, Object>) x)
            .map(x -> new DeleteDocumentsRequestData().id((String) x.get("_id")))
            .collect(Collectors.toList());

    DeleteDocumentsRequest req = new DeleteDocumentsRequest().data(allDocIds);
    DeleteDocumentsResponse resp = client.deleteDocuments(workspace, cname, req);
    // TODO handle resp

  }
}
