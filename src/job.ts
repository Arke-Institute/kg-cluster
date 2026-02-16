/**
 * Job Processing Logic for KG Clustering
 *
 * Implements the "Relationship-Based Discovery" algorithm:
 * 1. Get entity's layer
 * 2. Semantic search for similar peers (filtered by layer)
 * 3. Parallel fetch peers to check relationships
 * 4. If peer has summarized_by → join that cluster
 * 5. Else: create own cluster
 */

import type { ArkeClient } from '@arke-institute/sdk';
import type { KladosLogger, KladosRequest, Output } from '@arke-institute/rhiza';
import type { Env, TargetProperties, ClusterInputProperties } from './types';
import { findSimilarPeers } from './semantic';
import {
  fetchPeers,
  findExistingCluster,
  createCluster,
  joinCluster,
  getClusterMemberCount,
  dissolveCluster,
} from './cluster';

// Default wait parameters
const DEFAULT_FOLLOWER_WAIT_MS = 90000; // 90 seconds
const DEFAULT_FOLLOWER_POLL_INTERVAL_MS = 10000; // 10 seconds

/**
 * Sleep for a given number of milliseconds
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export interface ProcessContext {
  request: KladosRequest;
  client: ArkeClient;
  logger: KladosLogger;
  sql: SqlStorage;
  env: Env;
}

export interface ProcessResult {
  outputs?: Output[];
  reschedule?: boolean;
}

/**
 * Wait for followers to join a newly created cluster.
 * If followers join, returns the clusterId to propagate.
 * If still alone after timeout, dissolves the cluster and returns null.
 */
async function waitForFollowers(
  client: ArkeClient,
  logger: KladosLogger,
  entityId: string,
  clusterId: string,
  maxWaitMs: number,
  pollIntervalMs: number
): Promise<string | null> {
  const startTime = Date.now();

  logger.info('Waiting for followers', {
    clusterId,
    maxWaitMs,
    pollIntervalMs,
  });

  while (Date.now() - startTime < maxWaitMs) {
    await sleep(pollIntervalMs);

    const memberCount = await getClusterMemberCount(client, clusterId);
    const elapsed = Math.round((Date.now() - startTime) / 1000);

    if (memberCount > 1) {
      logger.info('Followers joined cluster', {
        clusterId,
        memberCount,
        elapsedSec: elapsed,
      });
      return clusterId;
    }

    logger.info('Still waiting for followers', {
      clusterId,
      memberCount,
      elapsedSec: elapsed,
    });
  }

  // Timeout - still alone, dissolve the cluster
  logger.info('No followers after timeout, dissolving solo cluster', {
    clusterId,
    waitedMs: maxWaitMs,
  });

  await dissolveCluster(client, entityId, clusterId);
  return null;
}

/**
 * Process a clustering job
 *
 * Returns:
 * - If joined existing cluster: outputs = [] (no handoff to describe)
 * - If created new cluster with followers: outputs = [clusterId] (handoff to describe)
 * - If created cluster but no followers after wait: outputs = [] (done, hierarchy terminates)
 */
export async function processJob(ctx: ProcessContext): Promise<ProcessResult> {
  const { request, client, logger } = ctx;

  // Extract configurable wait parameters
  const inputProps = (request.input || {}) as ClusterInputProperties;
  const followerWaitMs = inputProps.follower_wait_ms ?? DEFAULT_FOLLOWER_WAIT_MS;
  const followerPollIntervalMs = inputProps.follower_poll_interval_ms ?? DEFAULT_FOLLOWER_POLL_INTERVAL_MS;

  // ═══════════════════════════════════════════════════════════════════════════
  // STEP 1: Fetch Target Entity and Get Layer
  // ═══════════════════════════════════════════════════════════════════════════
  logger.info('Starting clustering');

  if (!request.target_entity) {
    throw new Error('No target_entity in request');
  }

  const { data: target, error: fetchError } = await client.api.GET('/entities/{id}', {
    params: { path: { id: request.target_entity } },
  });

  if (fetchError || !target) {
    throw new Error(`Failed to fetch target: ${request.target_entity}`);
  }

  const properties = target.properties as TargetProperties;
  const myLayer = properties._kg_layer ?? 0;

  logger.info('Fetched target', {
    id: target.id,
    type: target.type,
    label: properties.label,
    layer: myLayer,
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // STEP 2: Semantic Search for Similar Peers (Filtered by Layer)
  // ═══════════════════════════════════════════════════════════════════════════
  logger.info('Searching for similar peers', { layer: myLayer });

  const similarPeers = await findSimilarPeers(
    client,
    request.target_collection,
    {
      id: target.id,
      label: properties.label,
      description: properties.description as string | undefined,
    },
    myLayer,
    15
  );

  if (similarPeers.length === 0) {
    // No similar peers - create cluster and wait for potential followers
    logger.info('No similar peers found, creating cluster and waiting for followers');

    const clusterId = await createCluster(
      client,
      request.target_collection,
      target.id,
      myLayer
    );
    await joinCluster(client, target.id, clusterId);

    // Wait for followers - if none join, cluster is dissolved
    const survivingClusterId = await waitForFollowers(
      client,
      logger,
      target.id,
      clusterId,
      followerWaitMs,
      followerPollIntervalMs
    );

    if (survivingClusterId) {
      logger.success('Cluster has followers, propagating', { clusterId: survivingClusterId });
      return { outputs: [survivingClusterId] };
    } else {
      logger.success('Solo cluster dissolved, hierarchy terminates here');
      return { outputs: [] };
    }
  }

  logger.info('Found similar peers', { count: similarPeers.length });

  // ═══════════════════════════════════════════════════════════════════════════
  // STEP 3: Parallel Peer Fetch
  // ═══════════════════════════════════════════════════════════════════════════
  logger.info('Fetching peer entities');

  const peers = await fetchPeers(client, similarPeers);
  logger.info('Fetched peers', { count: peers.length });

  // ═══════════════════════════════════════════════════════════════════════════
  // STEP 4: Check for Clustered Peers (summarized_by)
  // ═══════════════════════════════════════════════════════════════════════════

  const existingCluster = findExistingCluster(peers);

  if (existingCluster) {
    logger.info('Found existing cluster via peer', {
      clusterId: existingCluster.clusterId,
      peerId: existingCluster.peerId,
    });

    await joinCluster(client, target.id, existingCluster.clusterId);
    logger.success('Joined existing cluster', {
      clusterId: existingCluster.clusterId,
    });

    // No handoff to describe - just complete
    return { outputs: [] };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // STEP 5: Create Own Cluster and Wait for Followers
  // ═══════════════════════════════════════════════════════════════════════════
  logger.info('No existing cluster found, creating new cluster');

  const clusterId = await createCluster(
    client,
    request.target_collection,
    target.id,
    myLayer
  );
  await joinCluster(client, target.id, clusterId);

  // Wait for followers - other similar peers may join this cluster
  const survivingClusterId = await waitForFollowers(
    client,
    logger,
    target.id,
    clusterId,
    followerWaitMs,
    followerPollIntervalMs
  );

  if (survivingClusterId) {
    logger.success('Cluster has followers, propagating', { clusterId: survivingClusterId });
    return { outputs: [survivingClusterId] };
  } else {
    logger.success('Solo cluster dissolved, hierarchy terminates here');
    return { outputs: [] };
  }
}
