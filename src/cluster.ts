/**
 * Cluster discovery and joining logic
 *
 * Implements the "Relationship-Based Discovery" algorithm:
 * 1. Fetch peers in parallel
 * 2. Check for summarized_by relationships
 * 3. If found, join that cluster
 * 4. If not found, create own cluster
 */

import type { ArkeClient } from '@arke-institute/sdk';
import type { KladosLogger } from '@arke-institute/rhiza';
import type { PeerEntity, ClusterMatch, SemanticCandidate, EntityInfo } from './types';
import { findSimilarPeers, isMutualPeer } from './semantic';

/**
 * Configuration for mutual K-nearest neighbors
 */
export interface MutualConfig {
  enabled: boolean;
  k: number;
  collection: string;
  myLayer: number;
}

/**
 * Fetch multiple peers in parallel
 */
export async function fetchPeers(
  client: ArkeClient,
  candidates: SemanticCandidate[]
): Promise<PeerEntity[]> {
  const peerPromises = candidates.map(async (c) => {
    try {
      const { data, error } = await client.api.GET('/entities/{id}', {
        params: { path: { id: c.id } },
      });
      if (error || !data) return null;
      return {
        id: data.id,
        type: data.type,
        properties: data.properties,
        relationships: data.relationships,
        similarity: c.similarity,
      } as PeerEntity;
    } catch {
      return null;
    }
  });

  const results = await Promise.all(peerPromises);
  return results.filter((r): r is PeerEntity => r !== null);
}

/**
 * Find existing cluster from peer relationships
 *
 * Checks each peer (in similarity order) for summarized_by relationship.
 * If mutual config is enabled, also verifies the peer has us in their top-K.
 *
 * @param client - Arke client (needed for mutual check)
 * @param myEntity - Our entity info
 * @param peers - Peer entities to check
 * @param mutualConfig - Mutual K-nearest neighbors configuration
 */
export async function findExistingCluster(
  client: ArkeClient,
  myEntity: EntityInfo,
  peers: PeerEntity[],
  mutualConfig: MutualConfig
): Promise<ClusterMatch | null> {
  // Sort by similarity (highest first)
  const sortedPeers = [...peers].sort((a, b) => b.similarity - a.similarity);

  for (const peer of sortedPeers) {
    if (!peer.relationships) continue;

    // Check for summarized_by (indicates peer is in a cluster)
    // Note: direction field may be undefined in API response, so just check predicate
    const clusterRel = peer.relationships.find(
      (r) => r.predicate === 'summarized_by'
    );

    if (clusterRel) {
      // If mutual mode enabled, verify peer also has us in their top-K
      if (mutualConfig.enabled) {
        const peerEntity: EntityInfo = {
          id: peer.id,
          label: peer.properties?.label,
          description: peer.properties?.description as string | undefined,
        };

        const isMutual = await isMutualPeer(
          client,
          mutualConfig.collection,
          myEntity,
          peerEntity,
          mutualConfig.myLayer,
          mutualConfig.k
        );

        if (!isMutual) {
          console.log(`[cluster] Skipping non-mutual peer ${peer.id} (they don't have us in top-${mutualConfig.k})`);
          continue; // Not mutual, try next peer
        }

        console.log(`[cluster] Mutual relationship confirmed with ${peer.id}`);
      }

      return {
        clusterId: clusterRel.peer,
        peerId: peer.id,
        similarity: peer.similarity,
      };
    }
  }

  return null;
}

/**
 * Create a new cluster entity
 */
export async function createCluster(
  client: ArkeClient,
  collection: string,
  anchorEntityId: string,
  myLayer: number
): Promise<string> {
  console.log(`[cluster] Creating cluster_${anchorEntityId} at layer ${myLayer + 1}`);

  const { data, error } = await (client.api.POST as Function)('/entities', {
    body: {
      type: 'cluster_leader',
      collection,
      properties: {
        label: `cluster_${anchorEntityId}`,
        _kg_layer: myLayer + 1,
      },
      sync_index: true,
    },
  });

  if (error || !data) {
    throw new Error(`Failed to create cluster: ${JSON.stringify(error)}`);
  }

  console.log(`[cluster] Created cluster: ${data.id}`);
  return data.id;
}

/**
 * Join a cluster (add bidirectional relationships)
 *
 * Uses CAS-safe updates with expect_tip.
 * If oldClusterId is provided, removes the old summarized_by in the same atomic operation.
 */
export async function joinCluster(
  client: ArkeClient,
  myEntityId: string,
  clusterId: string,
  oldClusterId?: string
): Promise<void> {
  console.log(`[cluster] ${myEntityId} joining ${clusterId}${oldClusterId ? ` (leaving ${oldClusterId})` : ''}`);

  // 1. Add summarized_by on self (enables discovery)
  // If switching clusters, also remove old summarized_by atomically
  const { data: myTip, error: myTipError } = await client.api.GET('/entities/{id}/tip', {
    params: { path: { id: myEntityId } },
  });

  if (myTipError || !myTip) {
    throw new Error(`Failed to get tip for ${myEntityId}: ${JSON.stringify(myTipError)}`);
  }

  // Build update body
  const updateBody: {
    expect_tip: string;
    relationships_add: Array<{ predicate: string; peer: string; direction: string }>;
    relationships_remove?: Array<{ predicate: string; peer: string }>;
  } = {
    expect_tip: myTip.cid,
    relationships_add: [
      {
        predicate: 'summarized_by',
        peer: clusterId,
        direction: 'outgoing',
      },
    ],
  };

  // If switching clusters, remove old relationship in same operation
  if (oldClusterId) {
    updateBody.relationships_remove = [
      {
        predicate: 'summarized_by',
        peer: oldClusterId,
      },
    ];
  }

  const { error: myUpdateError } = await client.api.PUT('/entities/{id}', {
    params: { path: { id: myEntityId } },
    body: updateBody,
  });

  if (myUpdateError) {
    throw new Error(`Failed to add summarized_by: ${JSON.stringify(myUpdateError)}`);
  }

  // 2. Add has_member on new cluster (enables gathering)
  // If switching clusters, also remove has_member from old cluster
  // Use fire-and-forget additive updates - handles CAS conflicts internally
  const additiveUpdates: Array<{
    entity_id: string;
    relationships_add?: Array<{ predicate: string; peer: string; direction: string }>;
    relationships_remove?: Array<{ predicate: string; peer: string }>;
  }> = [
    {
      entity_id: clusterId,
      relationships_add: [
        {
          predicate: 'has_member',
          peer: myEntityId,
          direction: 'outgoing',
        },
      ],
    },
  ];

  // Clean up old cluster's has_member relationship
  if (oldClusterId) {
    additiveUpdates.push({
      entity_id: oldClusterId,
      relationships_remove: [
        {
          predicate: 'has_member',
          peer: myEntityId,
        },
      ],
    });
  }

  const { error: additiveError } = await (client.api.POST as Function)('/updates/additive', {
    body: { updates: additiveUpdates },
  });

  if (additiveError) {
    // Log but don't fail - additive updates handle retries internally
    console.error(`[cluster] Additive update warning: ${JSON.stringify(additiveError)}`);
  }

  console.log(`[cluster] ${myEntityId} joined ${clusterId}`);
}

/**
 * Get the number of members in a cluster
 */
export async function getClusterMemberCount(
  client: ArkeClient,
  clusterId: string
): Promise<number> {
  const { data, error } = await client.api.GET('/entities/{id}', {
    params: { path: { id: clusterId } },
  });

  if (error || !data) {
    console.error(`[cluster] Failed to fetch cluster ${clusterId}: ${JSON.stringify(error)}`);
    return 0;
  }

  const members = (data.relationships || []).filter(
    (r) => r.predicate === 'has_member'
  );

  return members.length;
}

/**
 * Dissolve a solo cluster (remove relationships and delete cluster entity)
 *
 * 1. Remove summarized_by from the entity
 * 2. Delete the cluster entity
 */
export async function dissolveCluster(
  client: ArkeClient,
  entityId: string,
  clusterId: string
): Promise<void> {
  console.log(`[cluster] Dissolving solo cluster ${clusterId}`);

  // 1. Remove summarized_by from entity
  const { data: entityTip, error: tipError } = await client.api.GET('/entities/{id}/tip', {
    params: { path: { id: entityId } },
  });

  if (tipError || !entityTip) {
    console.error(`[cluster] Failed to get tip for ${entityId}: ${JSON.stringify(tipError)}`);
  } else {
    const { error: updateError } = await client.api.PUT('/entities/{id}', {
      params: { path: { id: entityId } },
      body: {
        expect_tip: entityTip.cid,
        relationships_remove: [
          {
            predicate: 'summarized_by',
            peer: clusterId,
          },
        ],
      },
    });

    if (updateError) {
      console.error(`[cluster] Failed to remove summarized_by: ${JSON.stringify(updateError)}`);
    }
  }

  // 2. Delete the cluster entity (requires expect_tip)
  const { data: clusterTip, error: clusterTipError } = await client.api.GET('/entities/{id}/tip', {
    params: { path: { id: clusterId } },
  });

  if (clusterTipError || !clusterTip) {
    console.error(`[cluster] Failed to get tip for cluster ${clusterId}: ${JSON.stringify(clusterTipError)}`);
    return;
  }

  const { error: deleteError } = await client.api.DELETE('/entities/{id}', {
    params: { path: { id: clusterId } },
    body: {
      expect_tip: clusterTip.cid,
    },
  });

  if (deleteError) {
    console.error(`[cluster] Failed to delete cluster ${clusterId}: ${JSON.stringify(deleteError)}`);
  } else {
    console.log(`[cluster] Dissolved cluster ${clusterId}`);
  }
}

/**
 * Leave a cluster (remove summarized_by relationship from entity)
 *
 * Call this before joining a different cluster to avoid orphaned relationships.
 */
export async function leaveCluster(
  client: ArkeClient,
  entityId: string,
  clusterId: string
): Promise<void> {
  console.log(`[cluster] ${entityId} leaving ${clusterId}`);

  const { data: tip, error: tipError } = await client.api.GET('/entities/{id}/tip', {
    params: { path: { id: entityId } },
  });

  if (tipError || !tip) {
    console.error(`[cluster] Failed to get tip for ${entityId}: ${JSON.stringify(tipError)}`);
    return;
  }

  const { error: updateError } = await client.api.PUT('/entities/{id}', {
    params: { path: { id: entityId } },
    body: {
      expect_tip: tip.cid,
      relationships_remove: [
        {
          predicate: 'summarized_by',
          peer: clusterId,
        },
      ],
    },
  });

  if (updateError) {
    console.error(`[cluster] Failed to remove summarized_by from ${entityId}: ${JSON.stringify(updateError)}`);
  } else {
    console.log(`[cluster] ${entityId} left ${clusterId}`);
  }
}

/**
 * Delete an empty cluster entity (when switching to another cluster)
 */
export async function deleteEmptyCluster(
  client: ArkeClient,
  clusterId: string
): Promise<void> {
  console.log(`[cluster] Deleting empty cluster ${clusterId}`);

  const { data: tip, error: tipError } = await client.api.GET('/entities/{id}/tip', {
    params: { path: { id: clusterId } },
  });

  if (tipError || !tip) {
    console.error(`[cluster] Failed to get tip for cluster ${clusterId}: ${JSON.stringify(tipError)}`);
    return;
  }

  const { error: deleteError } = await client.api.DELETE('/entities/{id}', {
    params: { path: { id: clusterId } },
    body: { expect_tip: tip.cid },
  });

  if (deleteError) {
    console.error(`[cluster] Failed to delete cluster ${clusterId}: ${JSON.stringify(deleteError)}`);
  } else {
    console.log(`[cluster] Deleted empty cluster ${clusterId}`);
  }
}

/**
 * Fallback result type
 */
export type FallbackResult = 'joined' | 'leader' | 'dissolved';

/**
 * Entity listing response type
 */
interface EntityListItem {
  id: string;
  type: string;
  properties?: Record<string, unknown>;
}

interface EntityListResponse {
  entities: EntityListItem[];
}

/**
 * Fallback clustering when no followers join within timeout.
 *
 * Two-phase fallback:
 * 1. SEMANTIC FALLBACK: Search for semantically similar peers (they should be indexed now)
 *    - If found peer with cluster → join their cluster (with mutual check if enabled)
 * 2. LEXICOGRAPHIC FALLBACK (last resort): Use fast SQLite index
 *    - Sort by ID, join first peer with cluster
 *    - If I'm first in order → stay leader
 *    - If truly alone → dissolve
 */
export async function fallbackJoinCluster(
  client: ArkeClient,
  entity: EntityInfo,
  myClusterId: string,
  collectionId: string,
  myLayer: number,
  logger: KladosLogger,
  mutualConfig?: MutualConfig
): Promise<FallbackResult> {
  const myEntityId = entity.id;
  logger.info('Starting fallback clustering', { myEntityId, myLayer });

  // =========================================================================
  // PHASE 1: SEMANTIC FALLBACK (entities should be indexed by now)
  // =========================================================================
  logger.info('Fallback phase 1: semantic search');

  // Search for similar peers with a larger K (20) since we're in fallback
  const similarPeers = await findSimilarPeers(
    client,
    collectionId,
    entity,
    myLayer,
    20 // Larger K for fallback to find more candidates
  );

  logger.info('Semantic fallback found peers', { count: similarPeers.length });

  // Fetch peers and check for clusters
  if (similarPeers.length > 0) {
    const peers = await fetchPeers(client, similarPeers);

    for (const peer of peers) {
      if (!peer.relationships) continue;

      const summarizedBy = peer.relationships.find(r => r.predicate === 'summarized_by');
      if (summarizedBy && summarizedBy.peer !== myClusterId) {
        // Found semantically similar peer with different cluster
        // Check mutual relationship if enabled
        if (mutualConfig?.enabled) {
          const peerEntity: EntityInfo = {
            id: peer.id,
            label: peer.properties?.label,
            description: peer.properties?.description as string | undefined,
          };

          const isMutual = await isMutualPeer(
            client,
            mutualConfig.collection,
            entity,
            peerEntity,
            mutualConfig.myLayer,
            mutualConfig.k
          );

          if (!isMutual) {
            logger.info('Semantic fallback: skipping non-mutual peer', { peerId: peer.id });
            continue; // Not mutual, try next peer
          }

          logger.info('Semantic fallback: mutual relationship confirmed', { peerId: peer.id });
        }

        // Join their cluster!
        const theirClusterId = summarizedBy.peer;
        logger.info('Semantic fallback: found similar peer with cluster', {
          peerId: peer.id,
          peerLabel: peer.properties?.label,
          theirClusterId,
          similarity: peer.similarity,
        });

        // Join their cluster (atomically removes old summarized_by)
        await joinCluster(client, myEntityId, theirClusterId, myClusterId);

        // Delete my now-orphaned cluster
        await deleteEmptyCluster(client, myClusterId);

        return 'joined';
      }
    }

    logger.info('Semantic fallback: no similar peers with clusters found');
  }

  // =========================================================================
  // PHASE 1.5: RETRY SEMANTIC FALLBACK (catch late-indexing entities)
  // =========================================================================
  // If mutual mode is enabled and first semantic check failed, wait briefly
  // and try again. This helps catch entities that were slow to index.
  if (mutualConfig?.enabled) {
    const retryDelayMs = 10000; // 10 seconds
    logger.info('Mutual mode: waiting before retry semantic check', { retryDelayMs });
    await new Promise(resolve => setTimeout(resolve, retryDelayMs));

    logger.info('Fallback phase 1.5: retry semantic search');

    const retryPeers = await findSimilarPeers(
      client,
      collectionId,
      entity,
      myLayer,
      20
    );

    logger.info('Retry semantic fallback found peers', { count: retryPeers.length });

    if (retryPeers.length > 0) {
      const peers = await fetchPeers(client, retryPeers);

      for (const peer of peers) {
        if (!peer.relationships) continue;

        const summarizedBy = peer.relationships.find(r => r.predicate === 'summarized_by');
        if (summarizedBy && summarizedBy.peer !== myClusterId) {
          const peerEntity: EntityInfo = {
            id: peer.id,
            label: peer.properties?.label,
            description: peer.properties?.description as string | undefined,
          };

          const isMutual = await isMutualPeer(
            client,
            mutualConfig.collection,
            entity,
            peerEntity,
            mutualConfig.myLayer,
            mutualConfig.k
          );

          if (!isMutual) {
            logger.info('Retry semantic fallback: skipping non-mutual peer', { peerId: peer.id });
            continue;
          }

          logger.info('Retry semantic fallback: mutual relationship confirmed', { peerId: peer.id });

          const theirClusterId = summarizedBy.peer;
          logger.info('Retry semantic fallback: found similar peer with cluster', {
            peerId: peer.id,
            peerLabel: peer.properties?.label,
            theirClusterId,
            similarity: peer.similarity,
          });

          await joinCluster(client, myEntityId, theirClusterId, myClusterId);
          await deleteEmptyCluster(client, myClusterId);

          return 'joined';
        }
      }

      logger.info('Retry semantic fallback: still no mutual peers with clusters');
    }

    // BUG: This causes infinite recursion for solo clusters.
    // Solo clusters return 'leader', output [clusterId], trigger describe → recurse → cluster again.
    //
    // TODO: The fix is NOT to fall through to lexicographic (that causes mega-clustering).
    // Instead, we need a better termination condition:
    // - Option 1: Dissolve if no peers found at this layer after semantic retry
    // - Option 2: Track recursion depth and dissolve at max depth
    // - Option 3: Check if this entity was already processed in a previous layer
    //
    // For now, returning 'leader' to preserve existing behavior, but this is broken
    // for recursive workflows.
    logger.info('Mutual mode enabled, no mutual peers found after retry - staying as leader');
    return 'leader';
  }

  logger.info('Fallback phase 2: lexicographic check');

  // List entities at same layer using fast SQLite index
  const filter = JSON.stringify({ _kg_layer: myLayer });
  const { data, error } = await (client.api.GET as Function)('/collections/{id}/entities', {
    params: {
      path: { id: collectionId },
      query: { filter, limit: 500 },
    },
  });

  if (error || !data) {
    logger.error('Lexicographic fallback listing failed', { error: JSON.stringify(error) });
    return 'leader'; // Keep cluster on error (safe default)
  }

  const response = data as EntityListResponse;
  const entities = response.entities || [];

  logger.info('Lexicographic fallback found entities at layer', { count: entities.length, layer: myLayer });

  // Sort lexicographically by ID (deterministic leader election)
  const sorted = [...entities].sort((a, b) => a.id.localeCompare(b.id));

  // Special case: I'm the only one → dissolve
  if (sorted.length === 1 && sorted[0].id === myEntityId) {
    logger.info('Only entity at this layer, dissolving');
    await dissolveCluster(client, myEntityId, myClusterId);
    return 'dissolved';
  }

  // Iterate through sorted list
  for (const ent of sorted) {
    // If I reach myself, I'm the leader (first in list without summarized_by)
    if (ent.id === myEntityId) {
      logger.info('Reached self in sorted list, becoming leader', { position: sorted.indexOf(ent) });
      return 'leader';
    }

    // Note: Do NOT skip cluster_leader entities - at Layer 1+, all entities are clusters
    // and we need to check if they have summarized_by pointing to a higher-layer cluster

    // Fetch full entity to check for summarized_by
    const { data: full, error: fetchError } = await client.api.GET('/entities/{id}', {
      params: { path: { id: ent.id } },
    });

    if (fetchError || !full) {
      logger.info('Failed to fetch peer, skipping', { peerId: ent.id });
      continue;
    }

    const relationships = (full as { relationships?: Array<{ predicate: string; peer: string }> }).relationships || [];
    const summarizedBy = relationships.find(r => r.predicate === 'summarized_by');

    if (summarizedBy) {
      // Found clustered peer via lexicographic order
      const theirClusterId = summarizedBy.peer;
      logger.info('Lexicographic fallback: found clustered peer', {
        peerId: ent.id,
        theirClusterId,
      });

      // Join their cluster (atomically removes old summarized_by)
      await joinCluster(client, myEntityId, theirClusterId, myClusterId);

      // Delete my now-orphaned cluster
      await deleteEmptyCluster(client, myClusterId);

      return 'joined';
    }
  }

  // Shouldn't reach here (would have hit myself in the loop), but keep cluster
  logger.info('Fallback loop completed without finding self or cluster');
  return 'leader';
}
