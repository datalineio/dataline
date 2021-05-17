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

package io.airbyte.integrations.destination.redshift;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.destination.jdbc.copy.SwitchingDestination;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Redshift Destination offers two replication strategies. The first inserts via a typical SQL
 * Insert statement. Although less efficient, this requires less user set up. See
 * {@link RedshiftInsertDestination} for more detail. The second inserts via streaming the data to
 * an S3 bucket, and Cop-ing the date into Redshift. This is more efficient, and recommended for
 * production workloads, but does require users to set up an S3 bucket and pass in additional
 * credentials. See {@link RedshiftCopyS3Destination} for more detail. This class inspect the given
 * arguments to determine which strategy to use.
 */
public class RedshiftDestination extends SwitchingDestination<RedshiftDestination.DestinationType> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftDestination.class);

  enum DestinationType {
    INSERT,
    COPY_S3
  }

  public RedshiftDestination() {
    super(DestinationType.class, RedshiftDestination::getTypeFromConfig, getTypeToDestination());
  }

  public static DestinationType getTypeFromConfig(JsonNode config) {
    if (isCopy(config)) {
      return DestinationType.COPY_S3;
    } else {
      return DestinationType.INSERT;
    }
  }

  public static Map<DestinationType, Destination> getTypeToDestination() {
    final RedshiftInsertDestination insertDestination = new RedshiftInsertDestination();
    final RedshiftCopyS3Destination copyS3Destination = new RedshiftCopyS3Destination();

    return ImmutableMap.of(
        DestinationType.INSERT, insertDestination,
        DestinationType.COPY_S3, copyS3Destination);
  }

  public static boolean isCopy(JsonNode config) {
    var bucketNode = config.get("s3_bucket_name");
    var regionNode = config.get("s3_bucket_region");
    var accessKeyIdNode = config.get("access_key_id");
    var secretAccessKeyNode = config.get("secret_access_key");

    // Since region is a Json schema enum with an empty string default, we consider the empty string an
    // unset field.
    var emptyRegion = regionNode == null || regionNode.asText().equals("");

    if (bucketNode == null && emptyRegion && accessKeyIdNode == null && secretAccessKeyNode == null) {
      return false;
    }

    if (bucketNode == null || regionNode == null || accessKeyIdNode == null || secretAccessKeyNode == null) {
      throw new RuntimeException("Error: Partially missing S3 Configuration.");
    }
    return true;
  }

  public static void main(String[] args) throws Exception {
    int mb = 1024 * 1024;
    MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    long xmx = memoryBean.getHeapMemoryUsage().getMax() / mb;
    long xms = memoryBean.getHeapMemoryUsage().getInit() / mb;
    LOGGER.info("Initial Memory (xms) : {}mb", xms);
    LOGGER.info("Max Memory (xmx) : {}mb", xmx);

    var service = Executors.newSingleThreadScheduledExecutor();
    service.scheduleAtFixedRate(() -> {
      LOGGER.info("Used heap memory: {}mb, Used non-heap memory: {}mb", memoryBean.getHeapMemoryUsage().getUsed() / mb, memoryBean.getNonHeapMemoryUsage().getUsed() / mb);
    }, 0, 20, TimeUnit.SECONDS);

    final Destination destination = new RedshiftDestination();
    LOGGER.info("starting destination: {}", RedshiftDestination.class);
    new IntegrationRunner(destination).run(args);
    LOGGER.info("completed destination: {}", RedshiftDestination.class);

    service.shutdown();
  }

}
