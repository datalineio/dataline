# Scaling Airbyte

As depicted in our [High-Level View](../understanding-airbyte/high-level-view.md), Airbyte is made up of several components under the hood:
1) Scheduler
2) Server
3) Temporal
4) Webapp
5) Database

These components perform control plane operations that are low-scale, low-resource work. In addition to the work being low cost, these components are efficient and optimized for these jobs, meaning that only uncommonly large workloads will require deployments at scale. In general, you would only encounter scaling issues when running over a thousand connections.

As a reference point, the typical Airbyte user has 5 - 20 connectors and 10 - 100 connections configured. Almost all of these connections are scheduled,
either hourly or daily, resulting in at most 100 concurrent jobs.

## What To Scale
[Workers](../understanding-airbyte/jobs.md) do all the heavy lifting within Airbyte. A worker is responsible for executing Airbyte operations (e.g. Discover, Read, Sync etc),
and is created on demand whenever these operations are requested. Thus, every job has a corresponding worker executing its work.

How a worker executes work depends on the Airbyte deployment. In the Docker deployment, an Airbyte worker spins up at least one Docker container. In the Kubernetes
deployment, an Airbyte worker will create at least one Kubernetes pod. The created resource (Docker container or Kubernetes pod) does all the actual work.

Thus, scaling Airbyte is a matter of ensuring that the Docker container or Kubernetes Pod running the jobs has sufficient resources to execute its work.

Jobs-wise, we are mainly concerned with Sync jobs when thinking about scale. Sync jobs sync data from sources to destinations and are the majority of jobs run. Sync jobs use two workers.
One worker reads from the source; the other worker writes to the destination.

There are two resources to be aware of:
1) Memory
2) Disk space

### Memory
As mentioned above, we are mainly concerned with scaling Sync jobs. Within a Sync job, the main memory culprit is the Source worker.

This is because the Source worker reads up to 10,000 records in memory. This can present problems for database sources with tables that have large row sizes. e.g. a table with an average row size of 0.5MBs will require 0.5 * 10000 / 1000 = 5GBs of RAM. See [this issue](https://github.com/airbytehq/airbyte/issues/3439) for more information.

Our Java connectors currently follow Java's default behaviour with container memory and will only use up to 1/4 of the host's allocated memory. On Docker agent with 8GBs of RAM configured, a Java connector limits itself to 2Gbs of RAM and will see Out-of-Memory exceptions if this goes higher.

Note that all Source database connectors are Java connectors. This means that users currently need to over-specify memory resource for Java connectors.

On Docker, this can be solved by giving the Docker agent more memory. See [here](https://stackoverflow.com/questions/44533319/how-to-assign-more-memory-to-docker-container) for instructions. You might need to switch to a node with more memory. On Kubernetes, this can be solved by using a node type with more memory.

Improving this behaviour is on our roadmap. Please see [this issue](https://github.com/airbytehq/airbyte/issues/3440) for more information.

### Disk Space
Airbyte uses backpressure to try to read the minimal amount of logs required. In the past, disk space was a large concern, but we've since deprecated the expensive on-disk queue approach.

However, disk space might become an issue for the following reasons:

1) Long-running syncs can produce a fair amount of logs from the Docker agent and Airbyte on Docker deployments. Some work has been done to minimize accidental logging, so this should no longer be an acute problem, but is still an open issue.
   
2) Although Airyte connector images aren't massive, they aren't exactly small either. The typical connector image is ~300MB. An Airbyte deployment with
multiple connectors can easily use up to 10GBs of disk space.

Because of this, we recommend allocating a minimum of 30GBs of disk space per node. Since storage is on the cheaper side, we'd recommend you be safe than sorry, so err on the side of over-provisioning.

### On Kubernetes  
Users running Airbyte Kubernetes also have to make sure the Kubernetes cluster can accommodate the number of pods Airbyte creates.

Airbyte creates an additional setup pod for each worker Kubernetes pod it creates. This `command-fetcher` pod is responsible for retrieving a worker's entrypoint and helps Airbyte model Kubernetes pod as a local process.
Although the `command-fetcher` pod is created before the worker pod, variable termination periods mean it can still be alive while the worker Kubernetes pod runs. This means every job requires at least **two** Kubernetes pods under the hood.
This means Sync jobs can use up to **four** Kubernetes pod at one time.

To be safe, make sure the Kubernetes cluster can schedule up to `2 x <number-of-possible-concurrent-connections>` pods at once. This is the worse case estimate, and most users should be fine with `2 x <number-of-possible-concurrent-connections>`
as a rule of thumb.

This is a **non-issue** for users running Airbyte Docker.

## Feedback

The advice here is best-effort and by no means comprehensive. Please reach out on Slack if anything doesn't make sense or if something can be improved.

If you've been running Airbyte in production and have more tips up your sleeve, please welcome contributions!
