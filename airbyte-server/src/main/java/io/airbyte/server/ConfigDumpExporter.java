/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.server;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.io.Archives;
import io.airbyte.commons.lang.CloseableConsumer;
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.commons.yaml.Yamls;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.scheduler.persistence.JobPersistence;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;

// TODO: Write a test case which compares the output dump with the output of ArchiveHandler export
// for the same data

/**
 * This class acts like export method of ArchiveHandler but the difference is 1. It takes a full
 * dump of whatever is available in the config directory without any schema validation. We dont want
 * schema validation because in case of automatic migration, the code that is going to do the schema
 * validation is from new version but the data in the config files is old. Thus schema validation
 * would fail. 2. Unlike ArchiveHandler, this doesn't take the dump of specific files but looks at
 * the config directory and takes the full dump of whatever is available
 */
public class ConfigDumpExporter {

  private static final String ARCHIVE_FILE_NAME = "airbyte_config_dump";
  private static final String CONFIG_FOLDER_NAME = "airbyte_config";
  private static final String DB_FOLDER_NAME = "airbyte_db";
  private static final String VERSION_FILE_NAME = "VERSION";
  private final ConfigRepository configRepository;
  private final JobPersistence jobPersistence;

  public ConfigDumpExporter(ConfigRepository configRepository, JobPersistence jobPersistence) {
    this.configRepository = configRepository;
    this.jobPersistence = jobPersistence;
  }

  public File dump() {
    try {
      final Path tempFolder = Files.createTempDirectory(Path.of("/tmp"), ARCHIVE_FILE_NAME);
      final File dump = Files.createTempFile(ARCHIVE_FILE_NAME, ".tar.gz").toFile();
      exportVersionFile(tempFolder);
      dumpConfigs(tempFolder);
      dumpDatabase(tempFolder);

      Archives.createArchive(tempFolder, dump.toPath());
      return dump;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void exportVersionFile(Path tempFolder) throws IOException {
    final String version = jobPersistence.getVersion().orElseThrow();
    final File versionFile = Files.createFile(tempFolder.resolve(VERSION_FILE_NAME)).toFile();
    FileUtils.writeStringToFile(versionFile, version, Charset.defaultCharset());
  }

  private void dumpDatabase(Path parentFolder) throws Exception {
    final Map<String, Stream<JsonNode>> tables = jobPersistence.dump();
    Files.createDirectories(parentFolder.resolve(DB_FOLDER_NAME));
    for (Map.Entry<String, Stream<JsonNode>> table : tables.entrySet()) {
      final Path tablePath = buildTablePath(parentFolder, table.getKey());
      writeTableToArchive(tablePath, table.getValue());
    }
  }

  private void writeTableToArchive(final Path tablePath, final Stream<JsonNode> tableStream)
      throws Exception {
    Files.createDirectories(tablePath.getParent());
    final BufferedWriter recordOutputWriter = new BufferedWriter(
        new FileWriter(tablePath.toFile()));
    final CloseableConsumer<JsonNode> recordConsumer = Yamls.listWriter(recordOutputWriter);
    tableStream.forEach(row -> Exceptions.toRuntime(() -> {
      recordConsumer.accept(row);
    }));
    recordConsumer.close();
  }

  protected static Path buildTablePath(final Path storageRoot, final String tableName) {
    return storageRoot
        .resolve(DB_FOLDER_NAME)
        .resolve(String.format("%s.yaml", tableName.toUpperCase()));
  }

  private void dumpConfigs(Path parentFolder) throws IOException {
    for (Map.Entry<String, Stream<JsonNode>> configEntry : configRepository.dumpConfigs().entrySet()) {
      writeConfigsToArchive(parentFolder, configEntry.getKey(), configEntry.getValue());
    }
  }

  private void writeConfigsToArchive(final Path storageRoot,
                                     final String schemaType,
                                     final Stream<JsonNode> configs)
      throws IOException {
    final Path configPath = buildConfigPath(storageRoot, schemaType);
    Files.createDirectories(configPath.getParent());
    final List<JsonNode> configList = configs.collect(Collectors.toList());
    if (!configList.isEmpty()) {
      final List<JsonNode> sortedConfigs = configList.stream()
          .sorted(Comparator.comparing(JsonNode::toString)).collect(
              Collectors.toList());
      Files.writeString(configPath, Yamls.serialize(sortedConfigs));
    } else {
      // Create empty file
      Files.createFile(configPath);
    }
  }

  private static Path buildConfigPath(final Path storageRoot, final String schemaType) {
    return storageRoot.resolve(CONFIG_FOLDER_NAME)
        .resolve(String.format("%s.yaml", schemaType));
  }

}
