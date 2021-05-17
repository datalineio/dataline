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

package io.airbyte.workers.process;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.io.LineGobbler;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.workers.WorkerException;
import io.airbyte.workers.WorkerUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DockerProcessBuilderFactory implements ProcessBuilderFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(DockerProcessBuilderFactory.class);

  private static final Path DATA_MOUNT_DESTINATION = Path.of("/data");
  private static final Path LOCAL_MOUNT_DESTINATION = Path.of("/local");
  private static final String IMAGE_EXISTS_SCRIPT = "image_exists.sh";

  private final String workspaceMountSource;
  private final Path workspaceRoot;
  private final String localMountSource;
  private final String networkName;
  private final Path imageExistsScriptPath;

  public DockerProcessBuilderFactory(Path workspaceRoot, String workspaceMountSource, String localMountSource, String networkName) {
    this.workspaceRoot = workspaceRoot;
    this.workspaceMountSource = workspaceMountSource;
    this.localMountSource = localMountSource;
    this.networkName = networkName;
    this.imageExistsScriptPath = prepareImageExistsScript();
  }

  private static Path prepareImageExistsScript() {
    try {
      final Path basePath = Files.createTempDirectory("scripts");
      final String scriptContents = MoreResources.readResource(IMAGE_EXISTS_SCRIPT);
      final Path scriptPath = IOs.writeFile(basePath, IMAGE_EXISTS_SCRIPT, scriptContents);
      if (!scriptPath.toFile().setExecutable(true)) {
        throw new RuntimeException(String.format("Could not set %s to executable", scriptPath));
      }
      return scriptPath;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ProcessBuilder create(String jobId, int attempt, final Path jobRoot, final String imageName, final String... args) throws WorkerException {

    if (!checkImageExists(imageName)) {
      throw new WorkerException("Could not find image: " + imageName);
    }
    LOGGER.info("Creating Docker job id: {}", jobId);
    final List<String> cmd =
        Lists.newArrayList(
            "docker",
            "run",
            "--rm",
            "--init",
            "-i",
            "-v",
            String.format("%s:%s", workspaceMountSource, DATA_MOUNT_DESTINATION),
            "-v",
            String.format("%s:%s", localMountSource, LOCAL_MOUNT_DESTINATION),
            "-w",
            rebasePath(jobRoot).toString());

    if (imageName.startsWith("airbyte/destination")) {
      LOGGER.info("Exposing image {} port", imageName);
      cmd.add("-p");
      cmd.add("6000:6000");
    }

    cmd.add(imageName);
    cmd.addAll(Arrays.asList(args));
    LOGGER.info("Command: {}", cmd);
    LOGGER.debug("Preparing command: {}", Joiner.on(" ").join(cmd));

    return new ProcessBuilder(cmd);
  }

  private Path rebasePath(final Path jobRoot) {
    final Path relativePath = workspaceRoot.relativize(jobRoot);
    return DATA_MOUNT_DESTINATION.resolve(relativePath);
  }

  @VisibleForTesting
  boolean checkImageExists(String imageName) throws WorkerException {
    try {
      final Process process = new ProcessBuilder(imageExistsScriptPath.toString(), imageName).start();
      LineGobbler.gobble(process.getErrorStream(), LOGGER::error);
      LineGobbler.gobble(process.getInputStream(), LOGGER::info);

      WorkerUtils.gentleClose(process, 10, TimeUnit.MINUTES);

      if (process.isAlive()) {
        throw new WorkerException("Process to check if image exists is stuck. Exiting.");
      } else {
        return process.exitValue() == 0;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
