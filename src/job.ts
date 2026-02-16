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
import type { Env, TargetProperties } from './types';
import { findSimilarPeers } from './semantic';
import {
  fetchPeers,
  findExistingCluster,
  createCluster,
  joinCluster,
} from './cluster';

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
 * Process a clustering job
 *
 * Returns:
 * - If joined existing cluster: outputs = [] (no handoff to describe)
 * - If created new cluster: outputs = [clusterId] (handoff to describe)
 */
export async function processJob(ctx: ProcessContext): Promise<ProcessResult> {
  const { request, client, logger } = ctx;

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
    // Solo cluster - no similar peers at this layer
    logger.info('No similar peers found, creating solo cluster');
    const clusterId = await createCluster(
      client,
      request.target_collection,
      target.id,
      myLayer
    );
    await joinCluster(client, target.id, clusterId);
    logger.success('Created solo cluster', { clusterId });
    return { outputs: [clusterId] }; // Pass to describe
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
  // STEP 5: Create Own Cluster
  // ═══════════════════════════════════════════════════════════════════════════
  logger.info('No existing cluster found, creating new cluster');

  const clusterId = await createCluster(
    client,
    request.target_collection,
    target.id,
    myLayer
  );
  await joinCluster(client, target.id, clusterId);

  logger.success('Created new cluster', { clusterId });

  // Pass to describe (only cluster creators do this)
  return { outputs: [clusterId] };
}
