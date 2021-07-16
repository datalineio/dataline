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

package io.airbyte.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import io.airbyte.api.model.*;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.scheduler.client.CachingSynchronousSchedulerClient;
import io.airbyte.scheduler.client.SynchronousResponse;
import io.airbyte.server.converters.SpecFetcher;
import io.airbyte.server.services.AirbyteGithubStore;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SourceDefinitionsHandlerTest {

  private ConfigRepository configRepository;
  private SpecFetcher specFetcher;
  private SynchronousResponse<ConnectorSpecification> jobResponse;
  private StandardSourceDefinition source;
  private SourceDefinitionsHandler sourceHandler;
  private Supplier<UUID> uuidSupplier;
  private CachingSynchronousSchedulerClient schedulerSynchronousClient;
  private AirbyteGithubStore githubStore;

  // Mocking the specFetcher means knowing about input images and output connector specifications
  private static final ConnectorSpecification CONNECTION_SPECIFICATION = new ConnectorSpecification()
      .withDocumentationUrl(Exceptions.toRuntime(() -> new URI("https://google.com")))
      .withChangelogUrl(Exceptions.toRuntime(() -> new URI("https://google.com")))
      .withConnectionSpecification(Jsons.jsonNode(new HashMap<>()));

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() {
    configRepository = mock(ConfigRepository.class);
    uuidSupplier = mock(Supplier.class);
    specFetcher = mock(SpecFetcher.class);
    jobResponse = mock(SynchronousResponse.class, RETURNS_DEEP_STUBS);
    schedulerSynchronousClient = spy(CachingSynchronousSchedulerClient.class);
    githubStore = mock(AirbyteGithubStore.class);

    source = generateSource();

    sourceHandler = new SourceDefinitionsHandler(configRepository, specFetcher, uuidSupplier, schedulerSynchronousClient, githubStore);
  }

  private StandardSourceDefinition generateSource() {
    final UUID sourceId = UUID.randomUUID();

    return new StandardSourceDefinition()
        .withSourceDefinitionId(sourceId)
        .withName("presto")
        .withDocumentationUrl("https://netflix.com")
        .withDockerRepository("dockerstuff")
        .withDockerImageTag("12.3")
        .withIcon("http.svg");
  }

  @Test
  @DisplayName("listSourceDefinition should return the right list")
  void testListSourceDefinitions() throws JsonValidationException, IOException, URISyntaxException {
    final StandardSourceDefinition source2 = generateSource();

    when(configRepository.listStandardSources()).thenReturn(Lists.newArrayList(source, source2));

    SourceDefinitionRead expectedSourceDefinitionRead1 = new SourceDefinitionRead()
        .sourceDefinitionId(source.getSourceDefinitionId())
        .name(source.getName())
        .dockerRepository(source.getDockerRepository())
        .dockerImageTag(source.getDockerImageTag())
        .documentationUrl(new URI(source.getDocumentationUrl()))
        .icon(SourceDefinitionsHandler.loadIcon(source.getIcon()));

    SourceDefinitionRead expectedSourceDefinitionRead2 = new SourceDefinitionRead()
        .sourceDefinitionId(source2.getSourceDefinitionId())
        .name(source2.getName())
        .dockerRepository(source.getDockerRepository())
        .dockerImageTag(source.getDockerImageTag())
        .documentationUrl(new URI(source.getDocumentationUrl()))
        .icon(SourceDefinitionsHandler.loadIcon(source.getIcon()));

    final SourceDefinitionReadList actualSourceDefinitionReadList = sourceHandler.listSourceDefinitions();

    assertEquals(Lists.newArrayList(expectedSourceDefinitionRead1, expectedSourceDefinitionRead2),
        actualSourceDefinitionReadList.getSourceDefinitions());
  }

  @Test
  @DisplayName("getSourceDefinition should return the right source")
  void testGetSourceDefinition() throws JsonValidationException, ConfigNotFoundException, IOException, URISyntaxException {
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()))
        .thenReturn(source);

    SourceDefinitionRead expectedSourceDefinitionRead = new SourceDefinitionRead()
        .sourceDefinitionId(source.getSourceDefinitionId())
        .name(source.getName())
        .dockerRepository(source.getDockerRepository())
        .dockerImageTag(source.getDockerImageTag())
        .documentationUrl(new URI(source.getDocumentationUrl()))
        .icon(SourceDefinitionsHandler.loadIcon(source.getIcon()));

    final SourceDefinitionIdRequestBody sourceDefinitionIdRequestBody =
        new SourceDefinitionIdRequestBody().sourceDefinitionId(source.getSourceDefinitionId());

    final SourceDefinitionRead actualSourceDefinitionRead = sourceHandler.getSourceDefinition(sourceDefinitionIdRequestBody);

    assertEquals(expectedSourceDefinitionRead, actualSourceDefinitionRead);
  }

  @Test
  @DisplayName("createSourceDefinition should correctly create a sourceDefinition")
  void testCreateSourceDefinition() throws URISyntaxException, IOException, JsonValidationException {
    final StandardSourceDefinition source = generateSource();
    when(uuidSupplier.get()).thenReturn(source.getSourceDefinitionId());
    final SourceDefinitionCreate create = new SourceDefinitionCreate()
        .name(source.getName())
        .dockerRepository(source.getDockerRepository())
        .dockerImageTag(source.getDockerImageTag())
        .documentationUrl(new URI(source.getDocumentationUrl()))
        .icon(source.getIcon());

    final SourceDefinitionRead expectedRead = new SourceDefinitionRead()
        .name(source.getName())
        .dockerRepository(source.getDockerRepository())
        .dockerImageTag(source.getDockerImageTag())
        .documentationUrl(new URI(source.getDocumentationUrl()))
        .sourceDefinitionId(source.getSourceDefinitionId())
        .icon(SourceDefinitionsHandler.loadIcon(source.getIcon()));

    when(specFetcher.executeWithResponse(source.getDockerRepository() + ":" + source.getDockerImageTag()))
        .thenReturn((SynchronousResponse<ConnectorSpecification>) jobResponse);
    when(jobResponse.getOutput()).thenReturn(CONNECTION_SPECIFICATION);
    when(jobResponse.isSuccess()).thenReturn(true);

    // Create makes the definition, validates the docker image as a side effect, and hands back the
    // jobInfo for logs.
    final SourceDefinitionReadWithJobInfo actualReadWithJobInfo = sourceHandler.createSourceDefinition(create);
    final SourceDefinitionRead actualRead = actualReadWithJobInfo.getSourceDefinitionRead();

    assertEquals(expectedRead, actualRead);
    assertNotNull(actualReadWithJobInfo.getJobInfo());
  }

  @Test
  @DisplayName("updateSourceDefinition should correctly update a sourceDefinition")
  void testUpdateSourceDefinition() throws ConfigNotFoundException, IOException, JsonValidationException {
    final String newDockerImageTag = "averydifferenttag";
    when(configRepository.getStandardSourceDefinition(source.getSourceDefinitionId())).thenReturn(source);
    when(specFetcher.executeWithResponse(source.getDockerRepository() + ":" + source.getDockerImageTag()))
        .thenReturn((SynchronousResponse<ConnectorSpecification>) jobResponse);
    when(specFetcher.executeWithResponse(source.getDockerRepository() + ":" + newDockerImageTag))
        .thenReturn((SynchronousResponse<ConnectorSpecification>) jobResponse);
    when(jobResponse.getOutput()).thenReturn(CONNECTION_SPECIFICATION);
    when(jobResponse.isSuccess()).thenReturn(true);

    final SourceDefinitionRead sourceDefinition = sourceHandler
        .getSourceDefinition(new SourceDefinitionIdRequestBody().sourceDefinitionId(source.getSourceDefinitionId()));
    final String dockerRepository = sourceDefinition.getDockerRepository();
    final String currentTag = sourceDefinition.getDockerImageTag();
    assertNotEquals(newDockerImageTag, currentTag);

    // This updates the source definition, validates the docker image as a side effect, and returns
    // jobInfo for logs.
    final SourceDefinitionReadWithJobInfo sourceDefinitionReadWithJobInfo = sourceHandler
        .updateSourceDefinition(new SourceDefinitionUpdate().sourceDefinitionId(source.getSourceDefinitionId()).dockerImageTag(newDockerImageTag));
    final SourceDefinitionRead sourceDefinitionRead = sourceDefinitionReadWithJobInfo.getSourceDefinitionRead();

    assertEquals(newDockerImageTag, sourceDefinitionRead.getDockerImageTag());
    assertNotNull(sourceDefinitionReadWithJobInfo.getJobInfo());

    verify(schedulerSynchronousClient).resetCache();
  }

  @Nested
  @DisplayName("listLatest")
  class listLatest {

    @Test
    @DisplayName("should return the latest list")
    void testCorrect() throws IOException, InterruptedException {
      final StandardSourceDefinition sourceDefinition = generateSource();
      when(githubStore.getLatestSources()).thenReturn(Collections.singletonList(sourceDefinition));

      final var sourceDefinitionReadList = sourceHandler.listLatestSourceDefinitions().getSourceDefinitions();
      assertEquals(1, sourceDefinitionReadList.size());

      final var sourceDefinitionRead = sourceDefinitionReadList.get(0);
      assertEquals(SourceDefinitionsHandler.buildSourceDefinitionRead(sourceDefinition), sourceDefinitionRead);
    }

    @Test
    @DisplayName("returns empty collection if cannot find latest definitions")
    void testHttpTimeout() {
      assertEquals(0, sourceHandler.listLatestSourceDefinitions().getSourceDefinitions().size());
    }

    @Test
    @DisplayName("Icon should contain data")
    void testIconHoldsData() {
      final String icon = SourceDefinitionsHandler.loadIcon(source.getIcon());
      assertNotNull(icon);
      assert (icon.length() > 3000);
      assert (icon.length() < 6000);
    }

  }

}
