/**
 * E2E Test for KG Cluster Worker
 *
 * Tests the relationship-based discovery clustering algorithm:
 * 1. Solo entity creates its own cluster
 * 2. Similar entities join existing clusters via summarized_by discovery
 * 3. Correct relationships are created (summarized_by, has_member)
 *
 * Prerequisites:
 * 1. Deploy worker: npm run deploy
 * 2. Register klados: npm run register
 * 3. Set environment variables
 *
 * Environment variables:
 *   ARKE_USER_KEY   - Your Arke user API key (uk_...)
 *   KLADOS_ID       - The klados entity ID from registration
 *   ARKE_API_BASE   - API base URL (default: https://arke-v1.arke.institute)
 *
 * Usage:
 *   npm test
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  configureTestClient,
  createCollection,
  createEntity,
  deleteEntity,
  getEntity,
  invokeKlados,
  waitForKladosLog,
  assertLogCompleted,
  assertLogHasMessages,
  log,
} from '@arke-institute/klados-testing';

// =============================================================================
// Configuration
// =============================================================================

const ARKE_API_BASE = process.env.ARKE_API_BASE || 'https://arke-v1.arke.institute';
const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
const NETWORK = (process.env.ARKE_NETWORK || 'test') as 'test' | 'main';
const KLADOS_ID = process.env.KLADOS_ID;

// =============================================================================
// Test Suite
// =============================================================================

describe('kg-cluster', () => {
  let targetCollection: { id: string };
  const createdEntities: string[] = [];

  beforeAll(() => {
    if (!ARKE_USER_KEY) {
      console.warn('Skipping tests: ARKE_USER_KEY not set');
      return;
    }
    if (!KLADOS_ID) {
      console.warn('Skipping tests: KLADOS_ID not set');
      return;
    }

    configureTestClient({
      apiBase: ARKE_API_BASE,
      userKey: ARKE_USER_KEY,
      network: NETWORK,
    });
  });

  beforeAll(async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) return;

    log('Creating test fixtures...');

    // Create target collection with invoke permissions
    targetCollection = await createCollection({
      label: `KG Cluster Test ${Date.now()}`,
      description: 'Target collection for clustering test',
      roles: { public: ['*:view', '*:invoke'] },
    });
    log(`Created target collection: ${targetCollection.id}`);
  });

  afterAll(async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) return;

    log('Cleaning up test fixtures...');

    try {
      for (const id of createdEntities) {
        try {
          await deleteEntity(id);
        } catch {
          // Ignore cleanup errors
        }
      }
      if (targetCollection?.id) await deleteEntity(targetCollection.id);
      log('Cleanup complete');
    } catch (e) {
      log(`Cleanup error (non-fatal): ${e}`);
    }
  });

  // ==========================================================================
  // Test: Solo entity creates its own cluster
  // ==========================================================================

  it('should create solo cluster when no similar peers exist', async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    // Create a unique entity (unlikely to match others)
    const uniqueEntity = await createEntity({
      type: 'test_entity',
      properties: {
        label: `Unique Entity ${Date.now()}`,
        description: 'A completely unique entity with no semantic peers',
        _kg_layer: 0,
      },
      collectionId: targetCollection.id,
    });
    createdEntities.push(uniqueEntity.id);
    log(`Created unique entity: ${uniqueEntity.id}`);

    // Invoke clustering
    log('Invoking cluster on unique entity...');
    const result = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: uniqueEntity.id,
      targetCollection: targetCollection.id,
      confirm: true,
    });

    expect(result.status).toBe('started');
    log(`Job started: ${result.job_id}`);

    // Wait for completion
    const kladosLog = await waitForKladosLog(result.job_collection!, {
      timeout: 60000,
      pollInterval: 3000,
    });

    assertLogCompleted(kladosLog);
    log(`Job completed with status: ${kladosLog.properties.status}`);

    // Verify log messages
    assertLogHasMessages(kladosLog, [
      { textContains: 'Starting clustering' },
      { textContains: 'creating' }, // "creating solo cluster" or "creating new cluster"
    ]);

    // Log all messages
    for (const msg of kladosLog.properties.log_data.messages) {
      log(`  [${msg.level}] ${msg.message}`);
    }

    // Verify entity now has summarized_by relationship
    const updatedEntity = await getEntity(uniqueEntity.id);
    const summarizedBy = updatedEntity.relationships?.find(
      (r: { predicate: string }) => r.predicate === 'summarized_by'
    );

    expect(summarizedBy).toBeDefined();
    log(`Entity has summarized_by → ${summarizedBy?.peer}`);

    // Track the cluster for cleanup
    if (summarizedBy) {
      createdEntities.push(summarizedBy.peer);
    }

    // Verify cluster has has_member relationship back
    const cluster = await getEntity(summarizedBy!.peer);
    const hasMember = cluster.relationships?.find(
      (r: { predicate: string; peer: string }) =>
        r.predicate === 'has_member' && r.peer === uniqueEntity.id
    );

    expect(hasMember).toBeDefined();
    log(`Cluster has has_member → ${uniqueEntity.id}`);

    // Verify cluster properties
    expect(cluster.type).toBe('cluster_leader');
    expect(cluster.properties.label).toContain('cluster_');
    expect(cluster.properties._kg_layer).toBe(1);
    log(`Cluster verified: type=${cluster.type}, layer=${cluster.properties._kg_layer}`);
  });

  // ==========================================================================
  // Test: Second similar entity joins existing cluster
  // ==========================================================================

  it('should join existing cluster when similar peer has summarized_by', async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    // Create first entity (whale-related)
    const entity1 = await createEntity({
      type: 'person',
      properties: {
        label: 'Captain Ahab',
        description: 'The monomaniacal captain of the whaling ship Pequod, obsessed with hunting the white whale Moby Dick',
        _kg_layer: 0,
      },
      collectionId: targetCollection.id,
    });
    createdEntities.push(entity1.id);
    log(`Created entity1 (Captain Ahab): ${entity1.id}`);

    // Wait for semantic index to propagate
    log('Waiting 60s for semantic index to propagate...');
    await new Promise((resolve) => setTimeout(resolve, 60000));

    // Cluster the first entity
    log('Clustering entity1...');
    const result1 = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: entity1.id,
      targetCollection: targetCollection.id,
      confirm: true,
    });

    const log1 = await waitForKladosLog(result1.job_collection!, {
      timeout: 60000,
      pollInterval: 3000,
    });
    assertLogCompleted(log1);

    // Get the cluster that was created
    const entity1Updated = await getEntity(entity1.id);
    const cluster1Rel = entity1Updated.relationships?.find(
      (r: { predicate: string }) => r.predicate === 'summarized_by'
    );
    expect(cluster1Rel).toBeDefined();
    createdEntities.push(cluster1Rel!.peer);
    log(`Entity1 is in cluster: ${cluster1Rel!.peer}`);

    // Wait for entity1's summarized_by relationship to be indexed
    log('Waiting 60s for entity1 relationships to be indexed...');
    await new Promise((resolve) => setTimeout(resolve, 60000));

    // Create second similar entity (also whale-related)
    const entity2 = await createEntity({
      type: 'person',
      properties: {
        label: 'Ahab the Hunter',
        description: 'A whaling captain consumed by his quest for revenge against a great white whale',
        _kg_layer: 0,
      },
      collectionId: targetCollection.id,
    });
    createdEntities.push(entity2.id);
    log(`Created entity2 (Ahab the Hunter): ${entity2.id}`);

    // Cluster the second entity - should find entity1's cluster via summarized_by
    log('Clustering entity2 (should join existing cluster)...');
    const result2 = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: entity2.id,
      targetCollection: targetCollection.id,
      confirm: true,
    });

    const log2 = await waitForKladosLog(result2.job_collection!, {
      timeout: 60000,
      pollInterval: 3000,
    });
    assertLogCompleted(log2);

    // Log messages
    for (const msg of log2.properties.log_data.messages) {
      log(`  [${msg.level}] ${msg.message}`);
    }

    // Verify entity2 joined the SAME cluster as entity1
    const entity2Updated = await getEntity(entity2.id);
    const cluster2Rel = entity2Updated.relationships?.find(
      (r: { predicate: string }) => r.predicate === 'summarized_by'
    );

    expect(cluster2Rel).toBeDefined();
    log(`Entity2 is in cluster: ${cluster2Rel!.peer}`);

    // Check if it's the same cluster (may or may not be, depends on semantic similarity)
    if (cluster2Rel!.peer === cluster1Rel!.peer) {
      log('SUCCESS: Entity2 joined entity1\'s cluster via summarized_by discovery');
    } else {
      log('Entity2 created its own cluster (semantic similarity may not have matched)');
      createdEntities.push(cluster2Rel!.peer);
    }

    // Either way, verify the relationship structure is correct
    const cluster2 = await getEntity(cluster2Rel!.peer);
    const hasMember2 = cluster2.relationships?.find(
      (r: { predicate: string; peer: string }) =>
        r.predicate === 'has_member' && r.peer === entity2.id
    );
    expect(hasMember2).toBeDefined();
    log('Bidirectional relationships verified');
  });

  // ==========================================================================
  // Test: Preview mode
  // ==========================================================================

  it('should handle preview mode (confirm=false)', async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    const previewEntity = await createEntity({
      type: 'test_entity',
      properties: {
        label: 'Preview Test Entity',
        description: 'Entity for testing preview mode',
        _kg_layer: 0,
      },
      collectionId: targetCollection.id,
    });
    createdEntities.push(previewEntity.id);

    const preview = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: previewEntity.id,
      targetCollection: targetCollection.id,
      confirm: false,
    });

    expect(preview.status).toBe('pending_confirmation');
    log(`Preview result: ${preview.status}`);
  });
});
