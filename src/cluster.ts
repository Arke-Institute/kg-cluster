/**
 * Cluster discovery and joining logic
 *
 * Implements the "Relationship-Based Discovery" algorithm:
 * 1. Fetch peers in parallel
 * 2. Check for summarized_by relationships
 * 3. If found, join that cluster
 * 4. If not found, create own cluster
 */

import { withCasRetry, type ArkeClient } from '@arke-institute/sdk';
import type { KladosLogger } from '@arke-institute/rhiza';
import type { PeerEntity, ClusterMatch, SemanticCandidate } from './types';

// Expected concurrent cluster joins (matches semantic search limit)
const CLUSTER_CONCURRENCY = 15;

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
 * Checks each peer (in similarity order) for summarized_by relationship
 */
export function findExistingCluster(peers: PeerEntity[]): ClusterMatch | null {
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
 * Uses CAS-safe updates with expect_tip
 */
export async function joinCluster(
  client: ArkeClient,
  myEntityId: string,
  clusterId: string
): Promise<void> {
  console.log(`[cluster] ${myEntityId} joining ${clusterId}`);

  // 1. Add summarized_by on self (enables discovery)
  const { data: myTip, error: myTipError } = await client.api.GET('/entities/{id}/tip', {
    params: { path: { id: myEntityId } },
  });

  if (myTipError || !myTip) {
    throw new Error(`Failed to get tip for ${myEntityId}: ${JSON.stringify(myTipError)}`);
  }

  const { error: myUpdateError } = await client.api.PUT('/entities/{id}', {
    params: { path: { id: myEntityId } },
    body: {
      expect_tip: myTip.cid,
      relationships_add: [
        {
          predicate: 'summarized_by',
          peer: clusterId,
          direction: 'outgoing',
        },
      ],
    },
  });

  if (myUpdateError) {
    throw new Error(`Failed to add summarized_by: ${JSON.stringify(myUpdateError)}`);
  }

  // 2. Add has_member on cluster (enables gathering)
  // Use withCasRetry for robust concurrent join handling
  const { attempts } = await withCasRetry(
    {
      getTip: async () => {
        const { data, error } = await client.api.GET('/entities/{id}/tip', {
          params: { path: { id: clusterId } },
        });
        if (error || !data) {
          throw new Error(`Failed to get tip for cluster ${clusterId}: ${JSON.stringify(error)}`);
        }
        return data.cid;
      },
      update: async (tip) => {
        return client.api.PUT('/entities/{id}', {
          params: { path: { id: clusterId } },
          body: {
            expect_tip: tip,
            relationships_add: [
              {
                predicate: 'has_member',
                peer: myEntityId,
                direction: 'outgoing',
              },
            ],
          },
        });
      },
    },
    {
      concurrency: CLUSTER_CONCURRENCY,
      onRetry: (attempt, _error, delayMs) => {
        console.log(`[cluster] CAS retry ${attempt} for ${clusterId}, waiting ${delayMs}ms`);
      },
    }
  );

  if (attempts > 1) {
    console.log(`[cluster] Joined ${clusterId} after ${attempts} attempts`);
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
 * Uses the fast SQLite-indexed collection listing (not semantic search)
 * to find peers at the same layer and join an existing cluster.
 *
 * Algorithm:
 * 1. List all entities at same _kg_layer (fast SQLite index)
 * 2. Sort lexicographically by ID (deterministic leader election)
 * 3. Iterate through sorted list:
 *    - If peer has summarized_by → join their cluster, delete mine
 *    - If I reach myself → I'm leader, keep my cluster
 * 4. If I'm the only entity at this layer → dissolve (truly alone)
 */
export async function fallbackJoinCluster(
  client: ArkeClient,
  myEntityId: string,
  myClusterId: string,
  collectionId: string,
  myLayer: number,
  logger: KladosLogger
): Promise<FallbackResult> {
  logger.info('Starting fallback clustering', { myEntityId, myLayer });

  // 1. List entities at same layer using fast SQLite index
  const filter = JSON.stringify({ _kg_layer: myLayer });
  const { data, error } = await (client.api.GET as Function)('/collections/{id}/entities', {
    params: {
      path: { id: collectionId },
      query: { filter, limit: 500 },
    },
  });

  if (error || !data) {
    logger.error('Fallback listing failed', { error: JSON.stringify(error) });
    return 'leader'; // Keep cluster on error (safe default)
  }

  const response = data as EntityListResponse;
  const entities = response.entities || [];

  logger.info('Fallback found entities at layer', { count: entities.length, layer: myLayer });

  // 2. Sort lexicographically by ID (deterministic leader election)
  const sorted = [...entities].sort((a, b) => a.id.localeCompare(b.id));

  // 3. Special case: I'm the only one → dissolve
  if (sorted.length === 1 && sorted[0].id === myEntityId) {
    logger.info('Only entity at this layer, dissolving');
    await dissolveCluster(client, myEntityId, myClusterId);
    return 'dissolved';
  }

  // 4. Iterate through sorted list
  for (const entity of sorted) {
    // If I reach myself, I'm the leader (first in list without summarized_by)
    if (entity.id === myEntityId) {
      logger.info('Reached self in sorted list, becoming leader', { position: sorted.indexOf(entity) });
      return 'leader';
    }

    // Skip cluster_leader entities (they're clusters, not members)
    if (entity.type === 'cluster_leader') {
      continue;
    }

    // Fetch full entity to check for summarized_by
    const { data: full, error: fetchError } = await client.api.GET('/entities/{id}', {
      params: { path: { id: entity.id } },
    });

    if (fetchError || !full) {
      logger.info('Failed to fetch peer, skipping', { peerId: entity.id });
      continue;
    }

    const relationships = (full as { relationships?: Array<{ predicate: string; peer: string }> }).relationships || [];
    const summarizedBy = relationships.find(r => r.predicate === 'summarized_by');

    if (summarizedBy) {
      // Found clustered peer! Join their cluster
      const theirClusterId = summarizedBy.peer;
      logger.info('Found clustered peer via fallback', {
        peerId: entity.id,
        theirClusterId,
      });

      // Join their cluster (adds summarized_by to me, has_member to cluster)
      await joinCluster(client, myEntityId, theirClusterId);

      // Delete my now-orphaned cluster
      await deleteEmptyCluster(client, myClusterId);

      return 'joined';
    }
  }

  // Shouldn't reach here (would have hit myself in the loop), but keep cluster
  logger.info('Fallback loop completed without finding self or cluster');
  return 'leader';
}
