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

package io.airbyte.config.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.Resources;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.MoreIterators;
import io.airbyte.commons.yaml.Yamls;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.init.SeedRepository;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This config persistence contains all seed definitions according to the yaml files. It is
 * read-only. This class can eventually replace the generateSeed task and the file system config
 * persistence.
 */
public class YamlSeedConfigPersistence implements ConfigPersistence {

  private enum SeedConfigType {

    STANDARD_WORKSPACE("/seed/workspace_definitions.yaml", "workspaceId"),
    STANDARD_SOURCE_DEFINITION("/seed/source_definitions.yaml", "sourceDefinitionId"),
    STANDARD_DESTINATION_DEFINITION("/seed/destination_definitions.yaml", "destinationDefinitionId");

    final String resourcePath;
    final String idName;

    SeedConfigType(String resourcePath, String idName) {
      this.resourcePath = resourcePath;
      this.idName = idName;
    }

  }

  private static final Map<ConfigSchema, SeedConfigType> CONFIG_SCHEMA_MAP = Map.of(
      ConfigSchema.STANDARD_WORKSPACE, SeedConfigType.STANDARD_WORKSPACE,
      ConfigSchema.STANDARD_SOURCE_DEFINITION, SeedConfigType.STANDARD_SOURCE_DEFINITION,
      ConfigSchema.STANDARD_DESTINATION_DEFINITION, SeedConfigType.STANDARD_DESTINATION_DEFINITION);

  // A mapping from seed config type to config UUID to config.
  private final Map<SeedConfigType, Map<String, JsonNode>> allSeedConfigs;

  public YamlSeedConfigPersistence() throws IOException {
    this.allSeedConfigs = new HashMap<>(3);
    allSeedConfigs.put(SeedConfigType.STANDARD_WORKSPACE, getConfigs(SeedConfigType.STANDARD_WORKSPACE));
    allSeedConfigs.put(SeedConfigType.STANDARD_SOURCE_DEFINITION, getConfigs(SeedConfigType.STANDARD_SOURCE_DEFINITION));
    allSeedConfigs.put(SeedConfigType.STANDARD_DESTINATION_DEFINITION, getConfigs(SeedConfigType.STANDARD_DESTINATION_DEFINITION));
  }

  private static Map<String, JsonNode> getConfigs(SeedConfigType seedConfigType) throws IOException {
    URL url = Resources.getResource(SeedRepository.class, seedConfigType.resourcePath);
    String yamlString = Resources.toString(url, StandardCharsets.UTF_8);
    JsonNode configList = Yamls.deserialize(yamlString);
    return MoreIterators.toList(configList.elements()).stream().collect(Collectors.toMap(
        json -> json.get(seedConfigType.idName).asText(),
        json -> json));
  }

  @Override
  public <T> T getConfig(ConfigSchema configType, String configId, Class<T> clazz)
      throws ConfigNotFoundException, JsonValidationException, IOException {
    Map<String, JsonNode> configs = allSeedConfigs.get(CONFIG_SCHEMA_MAP.get(configType));
    if (configs == null) {
      throw new UnsupportedOperationException("There is no seed for " + configType.name());
    }
    JsonNode config = configs.get(configId);
    if (config == null) {
      throw new ConfigNotFoundException(configType, configId);
    }
    return Jsons.object(config, clazz);
  }

  @Override
  public <T> List<T> listConfigs(ConfigSchema configType, Class<T> clazz) throws JsonValidationException, IOException {
    Map<String, JsonNode> configs = allSeedConfigs.get(CONFIG_SCHEMA_MAP.get(configType));
    if (configs == null) {
      throw new UnsupportedOperationException("There is no seed for " + configType.name());
    }
    return configs.values().stream().map(json -> Jsons.object(json, clazz)).collect(Collectors.toList());
  }

  @Override
  public <T> void writeConfig(ConfigSchema configType, String configId, T config) {
    throw new UnsupportedOperationException("The seed config persistence is read only.");
  }

  @Override
  public <T> void replaceAllConfigs(Map<ConfigSchema, Stream<T>> configs, boolean dryRun) {
    throw new UnsupportedOperationException("The seed config persistence is read only.");
  }

  @Override
  public Map<String, Stream<JsonNode>> dumpConfigs() {
    return allSeedConfigs.entrySet().stream().collect(Collectors.toMap(
        e -> e.getKey().name(),
        e -> e.getValue().values().stream()));
  }

}
