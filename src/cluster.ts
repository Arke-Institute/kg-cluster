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
import type { PeerEntity, ClusterMatch, SemanticCandidate } from './types';

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
  const { data: clusterTip, error: clusterTipError } = await client.api.GET('/entities/{id}/tip', {
    params: { path: { id: clusterId } },
  });

  if (clusterTipError || !clusterTip) {
    throw new Error(`Failed to get tip for cluster ${clusterId}: ${JSON.stringify(clusterTipError)}`);
  }

  const { error: clusterUpdateError } = await client.api.PUT('/entities/{id}', {
    params: { path: { id: clusterId } },
    body: {
      expect_tip: clusterTip.cid,
      relationships_add: [
        {
          predicate: 'has_member',
          peer: myEntityId,
          direction: 'outgoing',
        },
      ],
    },
  });

  if (clusterUpdateError) {
    throw new Error(`Failed to add has_member: ${JSON.stringify(clusterUpdateError)}`);
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

  // 2. Delete the cluster entity
  const { error: deleteError } = await client.api.DELETE('/entities/{id}', {
    params: { path: { id: clusterId } },
  });

  if (deleteError) {
    console.error(`[cluster] Failed to delete cluster ${clusterId}: ${JSON.stringify(deleteError)}`);
  } else {
    console.log(`[cluster] Dissolved cluster ${clusterId}`);
  }
}
