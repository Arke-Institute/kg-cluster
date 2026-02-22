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
  findJoinableCluster,
  createCluster,
  joinCluster,
  deleteEmptyCluster,
  getClusterMemberCount,
  fallbackJoinCluster,
  FallbackSearchConfig,
} from './cluster';

// Default wait parameters
// INDEX_WAIT: Minimum wait for semantic indexing before searching (entities must be indexed to be found)
// INITIAL_SPREAD: Additional random spread for staggering (creates dispersion so not all search at once)
// Total initial delay = INDEX_WAIT + random(0, INITIAL_SPREAD) = 30-60s
const DEFAULT_INDEX_WAIT_MS = 90000; // 90 seconds minimum - wait for semantic index (must exceed dedupe stage duration)
const DEFAULT_INITIAL_SPREAD_MS = 30000; // 30 seconds spread for staggering
const DEFAULT_FOLLOWER_WAIT_MIN_MS = 30000; // 30 seconds minimum
const DEFAULT_FOLLOWER_WAIT_MAX_MS = 60000; // 60 seconds maximum
const DEFAULT_FOLLOWER_POLL_INTERVAL_MS = 5000; // 5 seconds
const DEFAULT_RECHECK_DELAY_MS = 3000; // 3 seconds - delay before creating cluster to let concurrent jobs establish theirs
const DEFAULT_NO_PEERS_RETRY_DELAY_MS = 30000; // 30 seconds - delay before retrying if no peers found (indexer lag)
const DEFAULT_NO_PEERS_MAX_RETRIES = 2; // Maximum retries if no peers found

// Size-bounded clustering parameters
// K: Number of similar peers to fetch (default 15)
// MAX_CLUSTER_SIZE: Soft cap on cluster members (default 5)
// - When a cluster reaches MAX_SIZE, new entities overflow to neighboring clusters or create new ones
// - This prevents mega-clusters while keeping the algorithm O(n) instead of O(n*k)
const DEFAULT_K = 15;
const DEFAULT_MAX_CLUSTER_SIZE = 5;

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
 * After timeout, uses fallback clustering with FRESH semantic search
 * to discover late-indexed entities and find joinable clusters.
 */
async function waitForFollowers(
  client: ArkeClient,
  logger: KladosLogger,
  entity: EntityInfo,
  clusterId: string,
  minWaitMs: number,
  maxWaitMs: number,
  pollIntervalMs: number,
  peerIds: string[],
  maxClusterSize: number,
  searchConfig: FallbackSearchConfig
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

    // Check if peers have created joinable clusters we should join instead
    if (peerIds.length > 0) {
      const peerCandidates = peerIds.map((id) => ({ id, similarity: 1 }));
      const peers = await fetchPeers(client, peerCandidates);
      const joinable = await findJoinableCluster(client, peers, maxClusterSize);

      if (joinable && joinable.clusterId !== clusterId) {
        logger.info('Found peer cluster during wait, switching', {
          oldClusterId: clusterId,
          newClusterId: joinable.clusterId,
          peerId: joinable.peerId,
          elapsedSec: elapsed,
        });

        await joinCluster(client, entity.id, joinable.clusterId, clusterId);
        await deleteEmptyCluster(client, clusterId);
        return { action: 'joined' };
      }
    }

    logger.info('Still waiting for followers', {
      clusterId,
      memberCount,
      elapsedSec: elapsed,
    });
  }

  // Timeout - try fallback clustering with FRESH semantic search
  // This discovers late-indexed entities that weren't found in initial search
  logger.info('Timeout waiting for followers, attempting fallback clustering with fresh search');

  const fallbackResult = await fallbackJoinCluster(
    client,
    entity,
    clusterId,
    logger,
    searchConfig,
    maxClusterSize,
    findSimilarPeers
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
  const indexWaitMs = inputProps.index_wait_ms ?? DEFAULT_INDEX_WAIT_MS;
  const initialSpreadMs = inputProps.initial_spread_ms ?? DEFAULT_INITIAL_SPREAD_MS;
  const followerWaitMinMs = inputProps.follower_wait_min_ms ?? DEFAULT_FOLLOWER_WAIT_MIN_MS;
  const followerWaitMaxMs = inputProps.follower_wait_max_ms ?? DEFAULT_FOLLOWER_WAIT_MAX_MS;
  const followerPollIntervalMs = inputProps.follower_poll_interval_ms ?? DEFAULT_FOLLOWER_POLL_INTERVAL_MS;
  const k = inputProps.k ?? DEFAULT_K;
  const maxClusterSize = inputProps.max_cluster_size ?? DEFAULT_MAX_CLUSTER_SIZE;

  // ═══════════════════════════════════════════════════════════════════════════
  // STEP 0: Wait for Indexing + Staggered Start
  // ═══════════════════════════════════════════════════════════════════════════
  // First wait for semantic indexing (entities must be indexed to be found in search).
  // Then add random spread for dispersion (so not all entities search at exactly the same time).
  // Total delay = indexWaitMs + random(0, initialSpreadMs) = typically 30-60s
  const spreadDelay = Math.floor(Math.random() * initialSpreadMs);
  const totalDelay = indexWaitMs + spreadDelay;
  if (totalDelay > 0) {
    logger.info('Initial delay (index wait + spread)', {
      indexWaitMs,
      spreadDelayMs: spreadDelay,
      totalDelayMs: totalDelay
    });
    await sleep(totalDelay);
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
  logger.info('Searching for similar peers', { layer: myLayer, k, maxClusterSize });

  // Build entity info for this target
  const myEntity: EntityInfo = {
    id: target.id,
    label: properties.label,
    description: properties.description as string | undefined,
  };

  let similarPeers = await findSimilarPeers(
    client,
    request.target_collection,
    target.id,
    myLayer,
    k
  );

  // Retry if no peers found (semantic indexer may have lag)
  if (similarPeers.length === 0) {
    for (let retry = 1; retry <= DEFAULT_NO_PEERS_MAX_RETRIES; retry++) {
      logger.info('No peers found, waiting for indexer and retrying', {
        retry,
        maxRetries: DEFAULT_NO_PEERS_MAX_RETRIES,
        delayMs: DEFAULT_NO_PEERS_RETRY_DELAY_MS,
      });
      await sleep(DEFAULT_NO_PEERS_RETRY_DELAY_MS);

      similarPeers = await findSimilarPeers(
        client,
        request.target_collection,
        target.id,
        myLayer,
        k
      );

      if (similarPeers.length > 0) {
        logger.info('Found peers on retry', { retry, count: similarPeers.length });
        break;
      }
    }
  }

  if (similarPeers.length === 0) {
    // No similar peers after retries - terminate as singleton (truly alone)
    logger.info('No similar peers found after retries, terminating as singleton');
    return { outputs: [] };
  }

  logger.info('Found similar peers', { count: similarPeers.length });

  // ═══════════════════════════════════════════════════════════════════════════
  // STEP 3: Parallel Peer Fetch
  // ═══════════════════════════════════════════════════════════════════════════
  logger.info('Fetching peer entities');

  const peers = await fetchPeers(client, similarPeers);
  logger.info('Fetched peers', { count: peers.length });

  // ═══════════════════════════════════════════════════════════════════════════
  // STEP 4: Find Joinable Cluster (Size-Bounded)
  // ═══════════════════════════════════════════════════════════════════════════
  // Check peers (sorted by similarity) for one with a cluster that has room.
  // The size cap prevents mega-clusters while keeping the algorithm O(n).
  logger.info('Looking for joinable cluster');

  const peerIds = peers.map((p) => p.id);
  const joinable = await findJoinableCluster(client, peers, maxClusterSize);

  if (joinable) {
    logger.info('Found joinable cluster via peer', {
      clusterId: joinable.clusterId,
      peerId: joinable.peerId,
    });

    await joinCluster(client, target.id, joinable.clusterId);
    logger.success('Joined existing cluster', {
      clusterId: joinable.clusterId,
    });

    return { outputs: [] };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // STEP 5: Re-check After Delay (Race Condition Mitigation)
  // ═══════════════════════════════════════════════════════════════════════════
  // Wait briefly and re-check peers - concurrent jobs may have created clusters
  logger.info('No cluster found, waiting before re-check', { delayMs: DEFAULT_RECHECK_DELAY_MS });
  await sleep(DEFAULT_RECHECK_DELAY_MS);

  // Re-fetch peers to check cluster status
  const recheckCandidates = peerIds.map((id) => ({ id, similarity: 1 }));
  const recheckPeers = await fetchPeers(client, recheckCandidates);
  const recheckJoinable = await findJoinableCluster(client, recheckPeers, maxClusterSize);

  if (recheckJoinable) {
    logger.info('Found cluster on re-check', {
      clusterId: recheckJoinable.clusterId,
      peerId: recheckJoinable.peerId,
    });

    await joinCluster(client, target.id, recheckJoinable.clusterId);
    logger.success('Joined existing cluster after re-check', {
      clusterId: recheckJoinable.clusterId,
    });

    return { outputs: [] };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // STEP 6: Create Own Cluster and Wait for Followers
  // ═══════════════════════════════════════════════════════════════════════════
  logger.info('No existing cluster after re-check, creating new cluster');

  const clusterId = await createCluster(
    client,
    request.target_collection,
    target.id,
    myLayer
  );
  await joinCluster(client, target.id, clusterId);

  // Build search config for fallback (fresh semantic search)
  const searchConfig: FallbackSearchConfig = {
    collection: request.target_collection,
    myLayer,
    k,
  };

  // Wait for followers, with fallback clustering on timeout
  const result = await waitForFollowers(
    client,
    logger,
    myEntity,
    clusterId,
    followerWaitMinMs,
    followerWaitMaxMs,
    followerPollIntervalMs,
    peerIds,
    maxClusterSize,
    searchConfig
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
