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

package io.airbyte.migrate;

import io.airbyte.commons.io.Archives;
import io.airbyte.commons.version.AirbyteVersion;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrationRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(MigrationRunner.class);

  public static void run(String[] args) throws IOException {
    MigrateConfig migrateConfig = parse(args);
    run(migrateConfig);
  }

  public static void run(MigrateConfig migrateConfig) throws IOException {
    final Path workspaceRoot = Files.createTempDirectory(Path.of("/tmp"), "airbyte_migrate");
    migrateConfig = new MigrateConfig(migrateConfig.getInputPath(), migrateConfig.getOutputPath(),
        AirbyteVersion.versionWithoutPatch(migrateConfig.getTargetVersion()).getVersion());

    if (migrateConfig.getInputPath().toString().endsWith(".gz")) {
      LOGGER.info("Unpacking tarball");
      final Path uncompressedInputPath = Files.createDirectories(workspaceRoot.resolve("uncompressed"));
      Archives.extractArchive(migrateConfig.getInputPath(), uncompressedInputPath);
      migrateConfig = new MigrateConfig(
          uncompressedInputPath,
          migrateConfig.getOutputPath(),
          migrateConfig.getTargetVersion());
    }

    final Path outputPath = migrateConfig.getOutputPath();

    // todo hack
    migrateConfig = new MigrateConfig(
        migrateConfig.getInputPath(),
        workspaceRoot.resolve("output"),
        migrateConfig.getTargetVersion());

    LOGGER.info("Running migrations...");
    LOGGER.info(migrateConfig.toString());

    new Migrate(workspaceRoot.resolve("migrate")).run(migrateConfig);

    Archives.createArchive(migrateConfig.getOutputPath(), outputPath);

    LOGGER.info("Migration output written to {}", outputPath);
  }

  private static MigrateConfig parse(String[] args) {
    LOGGER.info("args: {}", Arrays.asList(args));
    final ArgumentParser parser = ArgumentParsers.newFor(Migrate.class.getName()).build()
        .defaultHelp(true)
        .description("Migrate Airbyte Data");

    parser.addArgument("--input")
        .required(true)
        .help("Path to .tar.gz archive or root dir of data to migrate.");

    parser.addArgument("--output")
        .required(true)
        .help("Full path of the output tarball. By convention should end with .tar.gz");

    parser.addArgument("--target-version")
        .required(false)
        .help("Version to upgrade the data to (default to latest migration available if left empty)");

    try {
      final Namespace parsed = parser.parseArgs(args);
      final Path inputPath = Path.of(parsed.getString("input"));
      final Path outputPath = Path.of(parsed.getString("output"));
      final String targetVersionFromCli = parsed.getString("target_version");
      final String targetVersion =
          Objects.isNull(targetVersionFromCli) ? Migrations.MIGRATIONS.get(Migrations.MIGRATIONS.size() - 1).getVersion() : targetVersionFromCli;
      return new MigrateConfig(inputPath, outputPath, targetVersion);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      throw new IllegalArgumentException(e);
    }
  }

  public static void main(String[] args) throws IOException {
    MigrationRunner.run(args);
  }

}
