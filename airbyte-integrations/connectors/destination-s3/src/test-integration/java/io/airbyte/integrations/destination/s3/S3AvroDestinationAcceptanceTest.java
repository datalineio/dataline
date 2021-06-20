package io.airbyte.integrations.destination.s3;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.destination.s3.avro.JsonFieldNameUpdater;
import io.airbyte.integrations.destination.s3.avro.JsonToAvroSchemaConverter;
import java.util.LinkedList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

public class S3AvroDestinationAcceptanceTest extends S3DestinationAcceptanceTest {

  private final JsonAvroConverter converter = new JsonAvroConverter();

  protected S3AvroDestinationAcceptanceTest() {
    super(S3Format.AVRO);
  }

  @Override
  protected JsonNode getFormatConfig() {
    return Jsons.deserialize("{\n"
        + "  \"format_type\": \"Avro\",\n"
        + "  \"compression_codec\": { \"codec\": \"no compression\", \"compression_level\": 5, \"include_checksum\": true }\n"
        + "}");
  }

  @Override
  protected List<JsonNode> retrieveRecords(TestDestinationEnv testEnv, String streamName, String namespace, JsonNode streamSchema) throws Exception {
    JsonToAvroSchemaConverter schemaConverter = new JsonToAvroSchemaConverter();
    schemaConverter.getAvroSchema(streamSchema, streamName, namespace, true);
    JsonFieldNameUpdater nameUpdater = new JsonFieldNameUpdater(schemaConverter.getStandardizedNames());

    List<S3ObjectSummary> objectSummaries = getAllSyncedObjects(streamName, namespace);
    List<JsonNode> jsonRecords = new LinkedList<>();

    for (S3ObjectSummary objectSummary : objectSummaries) {
      S3Object object = s3Client.getObject(objectSummary.getBucketName(), objectSummary.getKey());
      DataFileReader<Record> dataFileReader = new DataFileReader<>(
          new SeekableByteArrayInput(object.getObjectContent().readAllBytes()),
          new GenericDatumReader<>()
      );

      ObjectReader jsonReader = MAPPER.reader();
      while (dataFileReader.hasNext()) {
        GenericData.Record record = dataFileReader.next();
        byte[] jsonBytes = converter.convertToJson(record);
        JsonNode jsonRecord = jsonReader.readTree(jsonBytes);
        jsonRecord = nameUpdater.getJsonWithOriginalFieldNames(jsonRecord);
        jsonRecords.add(AvroRecordHelper.pruneAirbyteJson(jsonRecord));
      }
    }

    return jsonRecords;
  }

}
