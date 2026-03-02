/**
 * Cluster discovery and joining logic
 *
 * Implements Size-Bounded Semantic Clustering:
 * 1. Fetch peers in parallel
 * 2. Check for summarized_by relationships
 * 3. If found and cluster not full → join that cluster
 * 4. If all clusters full or no clusters → create own cluster
 */

import type { ArkeClient } from '@arke-institute/sdk';
import type { KladosLogger } from '@arke-institute/rhiza';
import type { PeerEntity, ClusterMatch, SemanticCandidate, EntityInfo } from './types';

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
 * Find a joinable cluster among peers (not full).
 *
 * Iterates through peers (sorted by similarity) and checks:
 * 1. Does peer have a summarized_by cluster?
 * 2. Is that cluster under the size cap?
 *
 * Returns the first cluster that has room, or null if none found.
 */
export async function findJoinableCluster(
  client: ArkeClient,
  peers: PeerEntity[],
  maxClusterSize: number
): Promise<ClusterMatch | null> {
  for (const peer of peers) {
    if (!peer.relationships) continue;

    const clusterRel = peer.relationships.find(
      (r) => r.predicate === 'summarized_by'
    );

    if (clusterRel) {
      // Check if cluster has room
      const size = await getClusterMemberCount(client, clusterRel.peer);

      if (size < maxClusterSize) {
        console.log(`[cluster] Found joinable cluster via peer ${peer.id}: ${clusterRel.peer} (${size}/${maxClusterSize})`);
        return {
          clusterId: clusterRel.peer,
          peerId: peer.id,
          similarity: peer.similarity,
        };
      } else {
        console.log(`[cluster] Cluster ${clusterRel.peer} is full (${size}/${maxClusterSize}), skipping`);
      }
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
  myLayer: number,
  anchorLabel?: string
): Promise<string> {
  // Generate a human-readable cluster label
  const clusterLabel = anchorLabel
    ? `Cluster: ${anchorLabel} (Layer ${myLayer + 1})`
    : `Cluster (Layer ${myLayer + 1})`;

  console.log(`[cluster] Creating "${clusterLabel}" for anchor ${anchorEntityId}`);

  const { data, error } = await (client.api.POST as Function)('/entities', {
    body: {
      type: 'cluster_leader',
      collection,
      properties: {
        label: clusterLabel,
        _kg_layer: myLayer + 1,
        anchor_entity_id: anchorEntityId,
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
  const { data: myTip, error: myTipError } = await client.api.GET('/entities/{id}/tip', {
    params: { path: { id: myEntityId } },
  });

  if (myTipError || !myTip) {
    throw new Error(`Failed to get tip for ${myEntityId}: ${JSON.stringify(myTipError)}`);
  }

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

  // 2. Add has_member on new cluster (additive update handles CAS conflicts)
  const { error: additiveError } = await (client.api.POST as Function)('/updates/additive', {
    body: {
      updates: [
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
      ],
    },
  });

  if (additiveError) {
    console.error(`[cluster] Additive update warning: ${JSON.stringify(additiveError)}`);
  }

  // 3. Clean up old cluster's has_member relationship (if switching clusters)
  if (oldClusterId) {
    try {
      const { data: oldClusterTip } = await client.api.GET('/entities/{id}/tip', {
        params: { path: { id: oldClusterId } },
      });

      if (oldClusterTip) {
        await client.api.PUT('/entities/{id}', {
          params: { path: { id: oldClusterId } },
          body: {
            expect_tip: oldClusterTip.cid,
            relationships_remove: [
              {
                predicate: 'has_member',
                peer: myEntityId,
              },
            ],
          },
        });
      }
    } catch (e) {
      console.error(`[cluster] Failed to clean up old cluster: ${e}`);
    }
  }

  console.log(`[cluster] ${myEntityId} joined ${clusterId}`);
}

/**
 * Leave a cluster (remove relationships).
 * Used when post-join size check shows we caused overflow.
 */
export async function leaveCluster(
  client: ArkeClient,
  myEntityId: string,
  clusterId: string
): Promise<void> {
  console.log(`[cluster] ${myEntityId} leaving ${clusterId}`);

  // 1. Remove summarized_by from self
  const { data: myTip, error: myTipError } = await client.api.GET('/entities/{id}/tip', {
    params: { path: { id: myEntityId } },
  });

  if (myTipError || !myTip) {
    console.error(`[cluster] Failed to get tip for ${myEntityId}: ${JSON.stringify(myTipError)}`);
    return;
  }

  const { error: removeError } = await client.api.PUT('/entities/{id}', {
    params: { path: { id: myEntityId } },
    body: {
      expect_tip: myTip.cid,
      relationships_remove: [
        {
          predicate: 'summarized_by',
          peer: clusterId,
        },
      ],
    },
  });

  if (removeError) {
    console.error(`[cluster] Failed to remove summarized_by: ${JSON.stringify(removeError)}`);
  }

  // 2. Remove has_member from cluster
  try {
    const { data: clusterTip } = await client.api.GET('/entities/{id}/tip', {
      params: { path: { id: clusterId } },
    });

    if (clusterTip) {
      await client.api.PUT('/entities/{id}', {
        params: { path: { id: clusterId } },
        body: {
          expect_tip: clusterTip.cid,
          relationships_remove: [
            {
              predicate: 'has_member',
              peer: myEntityId,
            },
          ],
        },
      });
    }
  } catch (e) {
    console.error(`[cluster] Failed to remove has_member from cluster: ${e}`);
  }

  console.log(`[cluster] ${myEntityId} left ${clusterId}`);
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

  // 2. Delete the cluster entity
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
 * Try to delete a cluster entity that should be empty (when switching to another cluster).
 *
 * Returns true if deletion succeeded, false if cluster has other members.
 * Uses single fetch for both member count AND CAS tip to prevent TOCTOU race.
 */
export async function tryDeleteEmptyCluster(
  client: ArkeClient,
  clusterId: string,
  myEntityId: string
): Promise<boolean> {
  console.log(`[cluster] Attempting to delete cluster ${clusterId}`);

  // Single fetch for BOTH member count AND CAS tip - prevents race condition
  // If we fetched member count separately, members could join before we get the tip,
  // causing us to delete a cluster that now has members.
  const { data: cluster, error: fetchError } = await client.api.GET('/entities/{id}', {
    params: { path: { id: clusterId } },
  });

  if (fetchError || !cluster) {
    console.error(`[cluster] Failed to fetch cluster ${clusterId}: ${JSON.stringify(fetchError)}`);
    return false;
  }

  // Check member count from THIS fetch
  const members = (cluster.relationships || []).filter(
    (r) => r.predicate === 'has_member'
  );

  if (members.length > 1) {
    console.log(`[cluster] Cluster ${clusterId} has ${members.length} members, not deleting`);
    return false;
  }

  // Delete using CID from THIS fetch - if anyone joined after our fetch, CAS will fail
  const { error: deleteError } = await client.api.DELETE('/entities/{id}', {
    params: { path: { id: clusterId } },
    body: { expect_tip: cluster.cid },
  });

  if (deleteError) {
    // CAS failure - someone joined between our fetch and delete
    console.log(`[cluster] Delete failed for ${clusterId} (concurrent join): ${JSON.stringify(deleteError)}`);
    return false;
  }

  console.log(`[cluster] Deleted empty cluster ${clusterId}`);
  return true;
}

/**
 * Fallback result type
 */
export type FallbackResult = 'joined' | 'leader' | 'dissolved';

/**
 * Fallback search config for fresh semantic searches
 */
export interface FallbackSearchConfig {
  collection: string;
  myLayer: number;
  k: number;
}

/**
 * Fallback clustering when no followers join within timeout.
 *
 * CRITICAL: Does a FRESH semantic search, not a re-fetch of stale peer IDs.
 * Entities that weren't indexed during the initial search will now be found.
 *
 * Only dissolves when the fresh search returns 0 results (truly alone).
 * If there are similar peers but no joinable cluster, stays as leader.
 */
export async function fallbackJoinCluster(
  client: ArkeClient,
  entity: EntityInfo,
  myClusterId: string,
  logger: KladosLogger,
  searchConfig: FallbackSearchConfig,
  maxClusterSize: number,
  findSimilarPeersFn: (
    client: ArkeClient,
    collection: string,
    entityId: string,
    myLayer: number,
    limit: number
  ) => Promise<SemanticCandidate[]>
): Promise<FallbackResult> {
  const myEntityId = entity.id;
  const { collection, myLayer, k } = searchConfig;

  logger.info('Starting fallback clustering with fresh search');

  // Phase 1: Fresh semantic search (discovers late-indexed entities)
  logger.info('Fallback: doing fresh semantic search');
  const freshPeers = await findSimilarPeersFn(client, collection, myEntityId, myLayer, k);

  if (freshPeers.length === 0) {
    // Truly alone - no similar entities exist. Safe to dissolve.
    logger.info('Fallback: fresh search found 0 peers, truly alone - dissolving');
    await dissolveCluster(client, myEntityId, myClusterId);
    return 'dissolved';
  }

  logger.info('Fallback: fresh search found peers', { count: freshPeers.length });

  // Phase 2: Check if any fresh peer has a joinable cluster
  const peers = await fetchPeers(client, freshPeers);
  const joinable = await findJoinableCluster(client, peers, maxClusterSize);

  if (joinable && joinable.clusterId !== myClusterId) {
    // Try to delete our cluster first - if others joined, we should stay as leader
    const deleted = await tryDeleteEmptyCluster(client, myClusterId, myEntityId);

    if (!deleted) {
      logger.info('Fallback: cluster has members, staying as leader', {
        clusterId: myClusterId,
      });
      return 'leader';
    }

    logger.info('Fallback: deleted empty cluster, joining peer cluster', {
      oldClusterId: myClusterId,
      newClusterId: joinable.clusterId,
      peerId: joinable.peerId,
    });

    await joinCluster(client, myEntityId, joinable.clusterId);
    return 'joined';
  }

  // Phase 3: Wait and retry (peers might still be creating clusters)
  const retryDelayMs = 15000;
  logger.info('Fallback: waiting before retry', { retryDelayMs });
  await new Promise((resolve) => setTimeout(resolve, retryDelayMs));

  // Phase 4: Check for followers OR joinable clusters one more time
  const memberCount = await getClusterMemberCount(client, myClusterId);

  if (memberCount > 1) {
    logger.info('Fallback: we have followers, staying as leader', { memberCount });
    return 'leader';
  }

  // Re-fetch peers to check for newly created clusters
  const retryPeers = await fetchPeers(client, freshPeers);
  const retryJoinable = await findJoinableCluster(client, retryPeers, maxClusterSize);

  if (retryJoinable && retryJoinable.clusterId !== myClusterId) {
    // Try to delete our cluster first - if others joined, we should stay as leader
    const deleted = await tryDeleteEmptyCluster(client, myClusterId, myEntityId);

    if (!deleted) {
      logger.info('Fallback retry: cluster has members, staying as leader', {
        clusterId: myClusterId,
      });
      return 'leader';
    }

    logger.info('Fallback retry: deleted empty cluster, joining peer cluster', {
      oldClusterId: myClusterId,
      newClusterId: retryJoinable.clusterId,
    });

    await joinCluster(client, myEntityId, retryJoinable.clusterId);
    return 'joined';
  }

  // Phase 5: We have similar peers but no one has a joinable cluster yet.
  // Stay as leader - peers will eventually find us.
  // DO NOT dissolve - that causes the "everyone dissolves" race condition.
  logger.info('Fallback: have peers but no joinable cluster, staying as leader', {
    peerCount: freshPeers.length,
  });
  return 'leader';
}
