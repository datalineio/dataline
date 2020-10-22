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

package io.airbyte.integrations.base;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.AirbyteCatalog;
import java.nio.file.Path;

/**
 * Extends TestSource such that it can be called using resources pulled from the file system. Will
 * also add the ability to execute arbitrary scripts in the next version.
 */
public class ExecutableTestSource extends TestSource {

  public static class TestConfig {

    private final String imageName;
    private final Path configPath;
    private final Path catalogPath;

    public TestConfig(String imageName, Path configPath, Path catalogPath) {
      this.imageName = imageName;
      this.configPath = configPath;
      this.catalogPath = catalogPath;
    }

    public Path getConfigPath() {
      return configPath;
    }

    public Path getCatalogPath() {
      return catalogPath;
    }

    public String getImageName() {
      return imageName;
    }

  }

  public static TestConfig TEST_CONFIG;

  @Override
  protected String getImageName() {
    return TEST_CONFIG.getImageName();
  }

  @Override
  protected JsonNode getConfig() {
    return Jsons.deserialize(IOs.readFile(TEST_CONFIG.getConfigPath()));
  }

  @Override
  protected AirbyteCatalog getCatalog() {
    return Jsons.deserialize(IOs.readFile(TEST_CONFIG.getCatalogPath()), AirbyteCatalog.class);
  }

  @Override
  protected void setup(TestDestinationEnv testEnv) throws Exception {
    // no-op, for now
  }

  @Override
  protected void tearDown(TestDestinationEnv testEnv) throws Exception {
    // no-op, for now
  }

}
