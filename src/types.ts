/**
 * Type definitions for the kg-cluster worker
 */

/**
 * Worker environment bindings
 */
export interface Env {
  AGENT_ID: string;
  AGENT_VERSION: string;
  ARKE_AGENT_KEY: string;
  VERIFICATION_TOKEN?: string;
  ARKE_VERIFY_AGENT_ID?: string;
  KLADOS_JOB: DurableObjectNamespace;
}

/**
 * Properties expected on target entities
 */
export interface TargetProperties {
  label?: string;
  description?: string;
  _kg_layer?: number;
  [key: string]: unknown;
}

/**
 * Configurable properties for clustering behavior
 */
export interface ClusterInputProperties {
  /** Minimum wait for semantic indexing before searching (ms, default: 30000). Entities must be indexed to be found. */
  index_wait_ms?: number;
  /** Additional random spread for staggering starts (ms, default: 30000). Total delay = index_wait + random(0, spread). */
  initial_spread_ms?: number;
  /** Minimum time to wait for followers (ms, default: 30000) */
  follower_wait_min_ms?: number;
  /** Maximum time to wait for followers (ms, default: 60000) */
  follower_wait_max_ms?: number;
  /** Interval between follower checks (ms, default: 5000) */
  follower_poll_interval_ms?: number;
}

/**
 * Entity info needed for semantic fallback
 */
export interface EntityInfo {
  id: string;
  label?: string;
  description?: string;
}

/**
 * Semantic search result from the API
 */
export interface SemanticCandidate {
  id: string;
  similarity: number;
  label?: string;
  type?: string;
}

/**
 * Peer entity fetched from the API with relationships
 * Note: GET /entities/{id} returns only outgoing relationships without direction field
 */
export interface PeerEntity {
  id: string;
  type: string;
  properties: TargetProperties;
  relationships?: Array<{
    predicate: string;
    peer: string;
    peer_label?: string;
    peer_type?: string;
  }>;
  similarity: number;
}

/**
 * Result of finding an existing cluster
 */
export interface ClusterMatch {
  clusterId: string;
  peerId: string;
  similarity: number;
}
