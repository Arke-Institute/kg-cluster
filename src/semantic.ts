/**
 * Semantic search utilities for finding similar peers
 */

import type { ArkeClient } from '@arke-institute/sdk';
import type { SemanticCandidate } from './types';

/**
 * Collection search response structure for similarity search
 * GET /collections/{id}/entities/search?similar_to=entityId
 */
interface SimilaritySearchResponse {
  collection_id: string;
  similar_to: string;
  entities: Array<{
    id: string;
    type: string;
    label?: string;
    score: number;
  }>;
  count: number;
}

/**
 * Find semantically similar entities at the same layer
 *
 * Uses the collection search endpoint with similar_to parameter
 * for entity-based semantic similarity search.
 *
 * Note: Layer filtering is done by fetching candidate entities and checking
 * their _kg_layer property. This is necessary because the search endpoint
 * returns similarity results across all layers.
 *
 * @param client - Arke client
 * @param collection - Collection to search in
 * @param entityId - Source entity ID for similarity search
 * @param myLayer - Current entity's _kg_layer value (for filtering)
 * @param limit - Max results (default 15)
 */
export async function findSimilarPeers(
  client: ArkeClient,
  collection: string,
  entityId: string,
  myLayer: number,
  limit: number = 15
): Promise<SemanticCandidate[]> {
  // Request more results than needed since we'll filter by layer
  const searchLimit = Math.min(limit * 3, 50);

  const { data, error } = await client.api.GET('/collections/{id}/entities/search', {
    params: {
      path: { id: collection },
      query: {
        similar_to: entityId,
        limit: searchLimit,
      },
    },
  });

  if (error || !data) {
    console.error('[semantic] Search failed:', error);
    return [];
  }

  const typedData = data as SimilaritySearchResponse;

  // Filter out self
  const candidates = typedData.entities.filter((r) => r.id !== entityId);

  if (candidates.length === 0) {
    return [];
  }

  // Fetch entities in parallel to check their _kg_layer
  const entityPromises = candidates.map(async (candidate) => {
    try {
      const { data: entityData, error: fetchError } = await client.api.GET('/entities/{id}', {
        params: { path: { id: candidate.id } },
      });
      if (fetchError || !entityData) return null;

      const props = entityData.properties as { _kg_layer?: number } | undefined;
      if (props?._kg_layer !== myLayer) return null;

      return {
        id: candidate.id,
        similarity: candidate.score,
        label: candidate.label,
        type: candidate.type,
      };
    } catch {
      return null;
    }
  });

  const results = await Promise.all(entityPromises);

  return results
    .filter((r): r is NonNullable<typeof r> => r !== null)
    .slice(0, limit);
}
