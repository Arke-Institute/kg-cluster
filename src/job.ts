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
import type { Env, TargetProperties, ClusterInputProperties, EntityInfo } from './types';
import { findSimilarPeers } from './semantic';
import {
  fetchPeers,
  findExistingCluster,
  createCluster,
  joinCluster,
  getClusterMemberCount,
  fallbackJoinCluster,
} from './cluster';

// Default wait parameters - jittery to create natural dispersion
// Longer defaults give entities more time to find each other via semantic search
// Semantic fallback catches entities that still miss each other after wait period
const DEFAULT_INITIAL_DELAY_MAX_MS = 30000; // 30 seconds max random initial delay to stagger starts
const DEFAULT_FOLLOWER_WAIT_MIN_MS = 30000; // 30 seconds minimum
const DEFAULT_FOLLOWER_WAIT_MAX_MS = 60000; // 60 seconds maximum
const DEFAULT_FOLLOWER_POLL_INTERVAL_MS = 5000; // 5 seconds
const DEFAULT_RECHECK_DELAY_MS = 3000; // 3 seconds - delay before creating cluster to let concurrent jobs establish theirs

/**
 * Generate a random wait time between min and max (jittery)
 * Creates natural dispersion - some entities finish waiting earlier
 */
function getJitteryWait(minMs: number, maxMs: number): number {
  return minMs + Math.floor(Math.random() * (maxMs - minMs));
}

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
 * Result from waitForFollowers
 */
type WaitResult =
  | { action: 'has_followers'; clusterId: string }
  | { action: 'leader'; clusterId: string }
  | { action: 'joined' }
  | { action: 'dissolved' };

/**
 * Wait for followers to join a newly created cluster.
 *
 * Uses jittery wait time to create natural dispersion.
 * After timeout, uses fallback clustering:
 * 1. First: Semantic search (entities should be indexed by now)
 * 2. Second: Lexicographic leader election (last resort)
 */
async function waitForFollowers(
  client: ArkeClient,
  logger: KladosLogger,
  entity: EntityInfo,
  clusterId: string,
  collectionId: string,
  myLayer: number,
  minWaitMs: number,
  maxWaitMs: number,
  pollIntervalMs: number
): Promise<WaitResult> {
  const startTime = Date.now();
  const jitteryWaitMs = getJitteryWait(minWaitMs, maxWaitMs);

  logger.info('Waiting for followers (jittery)', {
    clusterId,
    jitteryWaitMs,
    pollIntervalMs,
  });

  while (Date.now() - startTime < jitteryWaitMs) {
    await sleep(pollIntervalMs);

    const memberCount = await getClusterMemberCount(client, clusterId);
    const elapsed = Math.round((Date.now() - startTime) / 1000);

    if (memberCount > 1) {
      logger.info('Followers joined cluster', {
        clusterId,
        memberCount,
        elapsedSec: elapsed,
      });
      return { action: 'has_followers', clusterId };
    }

    logger.info('Still waiting for followers', {
      clusterId,
      memberCount,
      elapsedSec: elapsed,
    });
  }

  // Timeout - try fallback clustering (semantic first, then lexicographic)
  logger.info('Timeout waiting for followers, attempting fallback clustering');

  const fallbackResult = await fallbackJoinCluster(
    client,
    entity,
    clusterId,
    collectionId,
    myLayer,
    logger
  );

  switch (fallbackResult) {
    case 'joined':
      return { action: 'joined' };
    case 'leader':
      return { action: 'leader', clusterId };
    case 'dissolved':
      return { action: 'dissolved' };
  }
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

  // Extract configurable parameters
  const inputProps = (request.input || {}) as ClusterInputProperties;
  const initialDelayMaxMs = inputProps.initial_delay_max_ms ?? DEFAULT_INITIAL_DELAY_MAX_MS;
  const followerWaitMinMs = inputProps.follower_wait_min_ms ?? DEFAULT_FOLLOWER_WAIT_MIN_MS;
  const followerWaitMaxMs = inputProps.follower_wait_max_ms ?? DEFAULT_FOLLOWER_WAIT_MAX_MS;
  const followerPollIntervalMs = inputProps.follower_poll_interval_ms ?? DEFAULT_FOLLOWER_POLL_INTERVAL_MS;

  // ═══════════════════════════════════════════════════════════════════════════
  // STEP 0: Staggered Start (avoid race conditions)
  // ═══════════════════════════════════════════════════════════════════════════
  // Random initial delay creates natural dispersion. Early starters get indexed
  // before later starters search, allowing them to find and join existing clusters.
  if (initialDelayMaxMs > 0) {
    const delay = Math.floor(Math.random() * initialDelayMaxMs);
    logger.info('Staggered start delay', { delayMs: delay, maxDelayMs: initialDelayMaxMs });
    await sleep(delay);
  }

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
    5 // Smaller K creates more distinct clusters
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

    // Wait for followers, with fallback clustering on timeout
    const result = await waitForFollowers(
      client,
      logger,
      { id: target.id, label: properties.label, description: properties.description as string | undefined },
      clusterId,
      request.target_collection,
      myLayer,
      followerWaitMinMs,
      followerWaitMaxMs,
      followerPollIntervalMs
    );

    switch (result.action) {
      case 'has_followers':
        logger.success('Cluster has followers, propagating', { clusterId: result.clusterId });
        return { outputs: [result.clusterId] };
      case 'leader':
        logger.success('Became leader via fallback, propagating', { clusterId: result.clusterId });
        return { outputs: [result.clusterId] };
      case 'joined':
        logger.success('Joined existing cluster via fallback');
        return { outputs: [] };
      case 'dissolved':
        logger.success('Dissolved - only entity at this layer');
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
  // STEP 5: Re-check After Delay (Race Condition Mitigation)
  // ═══════════════════════════════════════════════════════════════════════════
  // Wait briefly and re-check peers - concurrent cluster jobs may have established their clusters
  logger.info('No existing cluster found, waiting before re-check', { delayMs: DEFAULT_RECHECK_DELAY_MS });
  await sleep(DEFAULT_RECHECK_DELAY_MS);

  // Re-fetch peers to check if any now have summarized_by
  const recheckPeers = await fetchPeers(client, similarPeers);
  const recheckCluster = findExistingCluster(recheckPeers);

  if (recheckCluster) {
    logger.info('Found cluster on re-check', {
      clusterId: recheckCluster.clusterId,
      peerId: recheckCluster.peerId,
    });

    await joinCluster(client, target.id, recheckCluster.clusterId);
    logger.success('Joined existing cluster after re-check', {
      clusterId: recheckCluster.clusterId,
    });

    return { outputs: [] };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // STEP 6: Create Own Cluster and Wait for Followers
  // ═══════════════════════════════════════════════════════════════════════════
  logger.info('Still no existing cluster after re-check, creating new cluster');

  const clusterId = await createCluster(
    client,
    request.target_collection,
    target.id,
    myLayer
  );
  await joinCluster(client, target.id, clusterId);

  // Wait for followers, with fallback clustering on timeout
  const result = await waitForFollowers(
    client,
    logger,
    { id: target.id, label: properties.label, description: properties.description as string | undefined },
    clusterId,
    request.target_collection,
    myLayer,
    followerWaitMinMs,
    followerWaitMaxMs,
    followerPollIntervalMs
  );

  switch (result.action) {
    case 'has_followers':
      logger.success('Cluster has followers, propagating', { clusterId: result.clusterId });
      return { outputs: [result.clusterId] };
    case 'leader':
      logger.success('Became leader via fallback, propagating', { clusterId: result.clusterId });
      return { outputs: [result.clusterId] };
    case 'joined':
      logger.success('Joined existing cluster via fallback');
      return { outputs: [] };
    case 'dissolved':
      logger.success('Dissolved - only entity at this layer');
      return { outputs: [] };
  }
}
