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

package io.airbyte.integrations.standardtest.source.fs;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.standardtest.source.SourceAcceptanceTest;
import io.airbyte.integrations.standardtest.source.TestDestinationEnv;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Extends TestSource such that it can be called using resources pulled from the file system. Will
 * also add the ability to execute arbitrary scripts in the next version.
 */
public class ExecutableTestSource extends SourceAcceptanceTest {

  public static class TestConfig {

    private final String imageName;
    private final Path specPath;
    private final Path configPath;
    private final Path catalogPath;

    private final Path statePath;

    public TestConfig(String imageName, Path specPath, Path configPath, Path catalogPath, Path statePath) {
      this.imageName = imageName;
      this.specPath = specPath;
      this.configPath = configPath;
      this.catalogPath = catalogPath;
      this.statePath = statePath;
    }

    public String getImageName() {
      return imageName;
    }

    public Path getSpecPath() {
      return specPath;
    }

    public Path getConfigPath() {
      return configPath;
    }

    public Path getCatalogPath() {
      return catalogPath;
    }

    @Nullable
    public Path getStatePath() {
      return statePath;
    }

  }

  public static TestConfig TEST_CONFIG;

  @Override
  protected ConnectorSpecification getSpec() {
    return Jsons.deserialize(IOs.readFile(TEST_CONFIG.getSpecPath()), ConnectorSpecification.class);
  }

  @Override
  protected String getImageName() {
    return TEST_CONFIG.getImageName();
  }

  @Override
  protected JsonNode getConfig() {
    return Jsons.deserialize(IOs.readFile(TEST_CONFIG.getConfigPath()));
  }

  @Override
  protected ConfiguredAirbyteCatalog getConfiguredCatalog() {
    return Jsons.deserialize(IOs.readFile(TEST_CONFIG.getCatalogPath()), ConfiguredAirbyteCatalog.class);
  }

  @Override
  protected JsonNode getState() {
    if (TEST_CONFIG.getStatePath() != null) {
      return Jsons.deserialize(IOs.readFile(TEST_CONFIG.getStatePath()));
    } else {
<<<<<<< HEAD:airbyte-integrations/bases/base-standard-fs-source-test/src/main/java/io/airbyte/integrations/standardtest/source/fs/ExecutableTestSource.java
      return Jsons.deserialize("{}}");
=======
      return Jsons.deserialize("{}");
>>>>>>> 8f30d665d1f4b68e87a0442541ec6c3dd1570459:airbyte-integrations/bases/base-standard-source-test-file/src/main/java/io/airbyte/integrations/standardtest/source/fs/ExecutableTestSource.java
    }

  }

  @Override
  protected List<String> getRegexTests() throws Exception {
    return new ArrayList<>();
  }

  @Override
  protected void setupEnvironment(TestDestinationEnv environment) throws Exception {
    // no-op, for now
  }

  @Override
  protected void tearDown(TestDestinationEnv testEnv) throws Exception {
    // no-op, for now
  }

}
