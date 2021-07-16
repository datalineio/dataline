package io.airbyte.integrations.destination.rockset;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rockset.client.RocksetClient;
import com.rockset.client.model.AddDocumentsRequest;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.DestinationSyncMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.airbyte.integrations.destination.rockset.RocksetUtils.*;

public class RocksetWriteApiConsumer implements AirbyteMessageConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(RocksetWriteApiConsumer.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  private final String apiKey;
  private final String workspace;

  private final ConfiguredAirbyteCatalog catalog;
  private final Consumer<AirbyteMessage> outputRecordCollector;

  private RocksetClient client;

  public RocksetWriteApiConsumer(
      JsonNode config,
      ConfiguredAirbyteCatalog catalog,
      Consumer<AirbyteMessage> outputRecordCollector) {
    this.apiKey = config.get(API_KEY_ID).asText();
    this.workspace = config.get(WORKSPACE_ID).asText();

    this.catalog = catalog;
    this.outputRecordCollector = outputRecordCollector;
  }

  @Override
  public void start() throws Exception {
    this.client = new RocksetClient(apiKey, APISERVER_URL);

    RocksetUtils.createWorkspaceIfNotExists(client, workspace);
        catalog.getStreams()
                .stream()
            .filter(s -> s.getDestinationSyncMode() == DestinationSyncMode.OVERWRITE)
            .forEach(s -> {
              try {
                RocksetUtils.deleteAllDocsInCollection(client, workspace, s.getStream().getName());
              } catch (Exception e) {
                e.printStackTrace();
              }
            });

    List<String> collectionNames =
            catalog.getStreams().stream()
                    .map(s -> s.getStream().getName())
                    .collect(Collectors.toList());

    for (String cname : collectionNames) {
      RocksetUtils.createCollectionIfNotExists(client, workspace, cname);
      RocksetUtils.waitUntilCollectionReady(client, workspace, cname);
    }
  }

  @Override
  public void accept(AirbyteMessage message) throws Exception {
    if (message.getType() == AirbyteMessage.Type.RECORD) {
      String cname = message.getRecord().getStream();

      AddDocumentsRequest req = new AddDocumentsRequest();
      req.addDataItem(mapper.convertValue(message.getRecord().getData(), new TypeReference<>() {}));

      this.client.addDocuments(workspace, cname, req);
    } else if (message.getType() == AirbyteMessage.Type.STATE) {
      this.outputRecordCollector.accept(message);
    }
  }

  @Override
  public void close() throws Exception {
    // Nothing to do
  }
}
