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

import com.github.slugify.Slugify;
import com.google.common.base.Strings;
import io.airbyte.analytics.TrackingClientSingleton;
import io.airbyte.api.model.ConnectionRead;
import io.airbyte.api.model.DestinationRead;
import io.airbyte.api.model.SlugRequestBody;
import io.airbyte.api.model.SourceRead;
import io.airbyte.api.model.WorkspaceCreate;
import io.airbyte.api.model.WorkspaceIdRequestBody;
import io.airbyte.api.model.WorkspaceRead;
import io.airbyte.api.model.WorkspaceUpdate;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.server.errors.KnownException;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.UUID;
import javax.ws.rs.NotFoundException;
import org.eclipse.jetty.http.HttpStatus;

public class WorkspacesHandler {

  private final ConfigRepository configRepository;
  private final ConnectionsHandler connectionsHandler;
  private final DestinationHandler destinationHandler;
  private final SourceHandler sourceHandler;
  private final Slugify slugify;

  public WorkspacesHandler(final ConfigRepository configRepository,
                           final ConnectionsHandler connectionsHandler,
                           final DestinationHandler destinationHandler,
                           final SourceHandler sourceHandler) {
    this.configRepository = configRepository;
    this.connectionsHandler = connectionsHandler;
    this.destinationHandler = destinationHandler;
    this.sourceHandler = sourceHandler;
    this.slugify = new Slugify();
  }

  public WorkspaceRead createWorkspace(final WorkspaceCreate workspaceCreate)
      throws JsonValidationException, IOException, ConfigNotFoundException, KnownException {

    final String email = workspaceCreate.getEmail();
    final Boolean initialSetupComplete = workspaceCreate.getInitialSetupComplete();
    final Boolean anonymousDataCollection = workspaceCreate.getAnonymousDataCollection();
    final Boolean news = workspaceCreate.getNews();
    final Boolean securityUpdates = workspaceCreate.getSecurityUpdates();
    final Boolean displaySetupWizard = workspaceCreate.getDisplaySetupWizard();

    final StandardWorkspace workspace = new StandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withCustomerId(UUID.randomUUID())
        .withName(workspaceCreate.getName())
        .withSlug(slugify.slugify(workspaceCreate.getName()))
        .withInitialSetupComplete(initialSetupComplete != null ? initialSetupComplete : false)
        .withAnonymousDataCollection(anonymousDataCollection != null ? anonymousDataCollection : false)
        .withNews(news != null ? news : false)
        .withSecurityUpdates(securityUpdates != null ? securityUpdates : false)
        .withDisplaySetupWizard(displaySetupWizard != null ? displaySetupWizard : false)
        .withTombstone(false);

    if (!Strings.isNullOrEmpty(email)) {
      workspace.withEmail(email);
    }

    try {
      if (getWorkspaceBySlug(workspace.getSlug()) != null) {
        throw new KnownException(HttpStatus.CONFLICT_409, "A workspace already exists with the same name");
      }
    } catch (NotFoundException e) {
      // if the workspace does not exist, we can create a new one
    }

    configRepository.writeStandardWorkspace(workspace);

    return buildWorkspaceRead(workspace);
  }

  public void deleteWorkspace(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    // get existing implementation
    final StandardWorkspace persistedWorkspace = configRepository.getStandardWorkspace(workspaceIdRequestBody.getWorkspaceId());

    // disable all connections associated with this workspace
    for (ConnectionRead connectionRead : connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody).getConnections()) {
      connectionsHandler.deleteConnection(connectionRead);
    }

    // disable all destinations associated with this workspace
    for (DestinationRead destinationRead : destinationHandler.listDestinationsForWorkspace(workspaceIdRequestBody).getDestinations()) {
      destinationHandler.deleteDestination(destinationRead);
    }

    // disable all sources associated with this workspace
    for (SourceRead sourceRead : sourceHandler.listSourcesForWorkspace(workspaceIdRequestBody).getSources()) {
      sourceHandler.deleteSource(sourceRead);
    }

    persistedWorkspace.withTombstone(true);
    configRepository.writeStandardWorkspace(persistedWorkspace);
  }

  public WorkspaceRead getWorkspace(WorkspaceIdRequestBody workspaceIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    return buildWorkspaceReadFromId(workspaceIdRequestBody.getWorkspaceId());
  }

  @SuppressWarnings("unused")
  public WorkspaceRead getWorkspaceBySlug(SlugRequestBody slugRequestBody) throws JsonValidationException, IOException, ConfigNotFoundException {
    // for now we assume there is one workspace and it has a default uuid.
    return buildWorkspaceReadFromId(PersistenceConstants.DEFAULT_WORKSPACE_ID);
  }

  public WorkspaceRead updateWorkspace(WorkspaceUpdate workspaceUpdate) throws ConfigNotFoundException, IOException, JsonValidationException {
    final UUID workspaceId = workspaceUpdate.getWorkspaceId();

    final StandardWorkspace persistedWorkspace = configRepository.getStandardWorkspace(workspaceId);

    if (!Strings.isNullOrEmpty(workspaceUpdate.getEmail())) {
      persistedWorkspace.withEmail(workspaceUpdate.getEmail());
    }

    persistedWorkspace
        .withInitialSetupComplete(workspaceUpdate.getInitialSetupComplete())
        .withDisplaySetupWizard(workspaceUpdate.getDisplaySetupWizard())
        .withAnonymousDataCollection(workspaceUpdate.getAnonymousDataCollection())
        .withNews(workspaceUpdate.getNews())
        .withSecurityUpdates(workspaceUpdate.getSecurityUpdates());

    configRepository.writeStandardWorkspace(persistedWorkspace);

    // after updating email or tracking info, we need to re-identify the instance.
    TrackingClientSingleton.get().identify();

    return buildWorkspaceReadFromId(workspaceUpdate.getWorkspaceId());
  }

  private WorkspaceRead buildWorkspaceReadFromId(UUID workspaceId) throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardWorkspace workspace = configRepository.getStandardWorkspace(workspaceId);
    return buildWorkspaceRead(workspace);
  }

  private WorkspaceRead buildWorkspaceRead(StandardWorkspace workspace)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    return new WorkspaceRead()
        .workspaceId(workspace.getWorkspaceId())
        .customerId(workspace.getCustomerId())
        .name(workspace.getName())
        .slug(workspace.getSlug())
        .initialSetupComplete(workspace.getInitialSetupComplete())
        .displaySetupWizard(workspace.getDisplaySetupWizard())
        .anonymousDataCollection(workspace.getAnonymousDataCollection())
        .news(workspace.getNews())
        .securityUpdates(workspace.getSecurityUpdates());
  }

}
