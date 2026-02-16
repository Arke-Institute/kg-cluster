/**
 * Semantic search utilities for finding similar peers
 */

import type { ArkeClient } from '@arke-institute/sdk';
import type { SemanticCandidate } from './types';

/**
 * Semantic search response structure
 * Results have entity data directly on each item when expand=preview
 */
interface SemanticSearchResponse {
  results: Array<{
    id: string;
    type?: string;
    label?: string;
    score: number;
  }>;
}

/**
 * Find semantically similar entities at the same layer
 *
 * @param client - Arke client
 * @param collection - Collection to search in
 * @param entity - Source entity (id, label, description)
 * @param myLayer - Current entity's _kg_layer value
 * @param limit - Max results (default 15)
 */
export async function findSimilarPeers(
  client: ArkeClient,
  collection: string,
  entity: { id: string; label?: string; description?: string },
  myLayer: number,
  limit: number = 15
): Promise<SemanticCandidate[]> {
  // Build query from label and description
  const queryParts: string[] = [];
  if (entity.label) queryParts.push(entity.label);
  if (entity.description) queryParts.push(entity.description);
  const query = queryParts.join(' ').trim();

  if (!query) {
    console.warn('[semantic] No query text for entity', entity.id);
    return [];
  }

  // POST to semantic search endpoint with _kg_layer filter
  // Using /search/entities for collection-scoped semantic search
  const { data, error } = await (client.api.POST as Function)('/search/entities', {
    body: {
      collection_id: collection,
      query,
      filter: { _kg_layer: myLayer },
      limit: limit + 1, // Extra to account for self
      expand: 'preview', // Get id, type, label
    },
  });

  if (error || !data) {
    console.error('[semantic] Search failed:', error);
    return [];
  }

  const typedData = data as SemanticSearchResponse;

  // Filter out self, map to candidates
  return typedData.results
    .filter((r) => r.id !== entity.id)
    .map((r) => ({
      id: r.id,
      similarity: r.score,
      label: r.label,
      type: r.type,
    }))
    .slice(0, limit);
}
