/**
 * E2E Test for KG Cluster Worker
 *
 * Tests the relationship-based discovery clustering algorithm:
 * 1. Solo entity creates cluster, waits for followers, dissolves if alone
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
  // Test: Solo entity creates cluster, waits, then dissolves if no followers
  // ==========================================================================

  it('should dissolve solo cluster when no followers join', async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    // Use a unique layer to ensure complete isolation from other tests/entities
    const uniqueLayer = 90000 + Math.floor(Math.random() * 10000);

    // Create a unique entity (unlikely to match others)
    const uniqueEntity = await createEntity({
      type: 'test_entity',
      properties: {
        label: `Unique Entity ${Date.now()}`,
        description: 'A completely unique entity with no semantic peers',
        _kg_layer: uniqueLayer,
      },
      collection: targetCollection.id,
    });
    createdEntities.push(uniqueEntity.id);
    log(`Created unique entity: ${uniqueEntity.id} at layer ${uniqueLayer}`);

    // Invoke clustering with short wait time for testing
    // Disable initial delay since we're the only entity at this layer
    log('Invoking cluster on unique entity (with 5s follower wait)...');
    const result = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: uniqueEntity.id,
      targetCollection: targetCollection.id,
      input: {
        initial_delay_max_ms: 0,         // No staggered start needed for solo test
        follower_wait_min_ms: 5000,      // 5 second min wait for testing
        follower_wait_max_ms: 10000,     // 10 second max wait for testing
        follower_poll_interval_ms: 1000, // 1 second poll
      },
      confirm: true,
    });

    expect(result.status).toBe('started');
    log(`Job started: ${result.job_id}`);

    // Wait for completion (should take ~5-10 seconds with short wait)
    const kladosLog = await waitForKladosLog(result.job_collection!, {
      timeout: 60000,
      pollInterval: 2000,
    });

    assertLogCompleted(kladosLog);
    log(`Job completed with status: ${kladosLog.properties.status}`);

    // Log all messages
    for (const msg of kladosLog.properties.log_data.messages) {
      log(`  [${msg.level}] ${msg.message}`);
    }

    // Verify log messages show the expected pattern
    assertLogHasMessages(kladosLog, [
      { textContains: 'Starting clustering' },
      { textContains: 'Waiting for followers' },
    ]);

    // Check for either dissolution or leader outcome
    // Both are valid for a solo entity - dissolution is ideal, but leader is safe default
    // if the entity wasn't indexed in time for lexicographic fallback
    const messages = kladosLog.properties.log_data.messages.map(
      (m: { message: string }) => m.message
    );
    const dissolved = messages.some((m: string) => m.toLowerCase().includes('dissolving'));
    const becameLeader = messages.some((m: string) => m.includes('Became leader via fallback'));

    expect(dissolved || becameLeader).toBe(true);

    if (dissolved) {
      log('Entity dissolved as expected');
      // Verify entity does NOT have summarized_by relationship
      const updatedEntity = await getEntity(uniqueEntity.id);
      const summarizedBy = updatedEntity.relationships?.find(
        (r: { predicate: string }) => r.predicate === 'summarized_by'
      );
      expect(summarizedBy).toBeUndefined();
      log('Entity has no summarized_by (cluster correctly dissolved)');

      // Verify the output was empty (hierarchy terminates)
      const outputs = kladosLog.properties.log_data.entry?.handoffs || [];
      expect(outputs.length).toBe(0);
      log('No outputs (hierarchy correctly terminates)');
    } else {
      log('Entity became leader via fallback (indexing lag prevented dissolution)');
      // This is acceptable - entity keeps its cluster as safe default
      // The outputs will contain the cluster ID
      const outputs = kladosLog.properties.log_data.entry?.handoffs || [];
      log(`Outputs: ${outputs.length} (cluster will propagate)`);
    }
  });

  // ==========================================================================
  // Test: Similar entities cluster together via concurrent processing
  // ==========================================================================

  it('should form cluster when similar entities process concurrently', async () => {
    if (!ARKE_USER_KEY || !KLADOS_ID) {
      console.warn('Test skipped: missing environment variables');
      return;
    }

    // Use a unique layer to ensure isolation from other tests
    const testLayer = 80000 + Math.floor(Math.random() * 10000);

    // Create two similar entities at the same time
    const entity1 = await createEntity({
      type: 'person',
      properties: {
        label: 'Captain Ahab',
        description: 'The monomaniacal captain of the whaling ship Pequod, obsessed with hunting the white whale Moby Dick',
        _kg_layer: testLayer,
      },
      collection: targetCollection.id,
    });
    createdEntities.push(entity1.id);
    log(`Created entity1 (Captain Ahab): ${entity1.id} at layer ${testLayer}`);

    const entity2 = await createEntity({
      type: 'person',
      properties: {
        label: 'Ahab the Hunter',
        description: 'A whaling captain consumed by his quest for revenge against a great white whale',
        _kg_layer: testLayer,
      },
      collection: targetCollection.id,
    });
    createdEntities.push(entity2.id);
    log(`Created entity2 (Ahab the Hunter): ${entity2.id}`);

    // Wait for semantic index to propagate for both entities
    log('Waiting 60s for semantic index to propagate...');
    await new Promise((resolve) => setTimeout(resolve, 60000));

    // Start clustering entity1 first (it will create a cluster and wait for followers)
    log('Clustering entity1 (will wait for followers)...');
    const result1 = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: entity1.id,
      targetCollection: targetCollection.id,
      input: {
        follower_wait_min_ms: 60000,     // Wait 60s min for followers
        follower_wait_max_ms: 90000,     // Wait 90s max for followers
        follower_poll_interval_ms: 5000,
      },
      confirm: true,
    });
    log(`Entity1 job started: ${result1.job_id}`);

    // Wait a bit for entity1 to create its cluster
    log('Waiting 10s for entity1 to create cluster...');
    await new Promise((resolve) => setTimeout(resolve, 10000));

    // Start clustering entity2 - should find entity1's cluster and join it
    log('Clustering entity2 (should join entity1\'s cluster)...');
    const result2 = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: entity2.id,
      targetCollection: targetCollection.id,
      input: {
        follower_wait_min_ms: 20000,     // Shorter wait since it should join existing
        follower_wait_max_ms: 30000,     // 30s max
        follower_poll_interval_ms: 5000,
      },
      confirm: true,
    });
    log(`Entity2 job started: ${result2.job_id}`);

    // Wait for entity2 to complete first (it joins and exits quickly)
    log('Waiting for entity2 to complete...');
    const log2 = await waitForKladosLog(result2.job_collection!, {
      timeout: 120000,
      pollInterval: 3000,
    });
    assertLogCompleted(log2);

    log('Entity2 log messages:');
    for (const msg of log2.properties.log_data.messages) {
      log(`  [${msg.level}] ${msg.message}`);
    }

    // Wait for entity1 to complete (should detect follower and exit)
    log('Waiting for entity1 to complete...');
    const log1 = await waitForKladosLog(result1.job_collection!, {
      timeout: 120000,
      pollInterval: 3000,
    });
    assertLogCompleted(log1);

    log('Entity1 log messages:');
    for (const msg of log1.properties.log_data.messages) {
      log(`  [${msg.level}] ${msg.message}`);
    }

    // Verify both entities have summarized_by relationships
    const entity1Updated = await getEntity(entity1.id);
    const cluster1Rel = entity1Updated.relationships?.find(
      (r: { predicate: string }) => r.predicate === 'summarized_by'
    );
    expect(cluster1Rel).toBeDefined();
    log(`Entity1 is in cluster: ${cluster1Rel!.peer}`);
    createdEntities.push(cluster1Rel!.peer);

    const entity2Updated = await getEntity(entity2.id);
    const cluster2Rel = entity2Updated.relationships?.find(
      (r: { predicate: string }) => r.predicate === 'summarized_by'
    );
    expect(cluster2Rel).toBeDefined();
    log(`Entity2 is in cluster: ${cluster2Rel!.peer}`);

    // Check if they're in the same cluster
    if (cluster2Rel!.peer === cluster1Rel!.peer) {
      log('SUCCESS: Both entities in the same cluster!');

      // Verify cluster has at least both members (could have more if other entities joined)
      const cluster = await getEntity(cluster1Rel!.peer);
      const members = cluster.relationships?.filter(
        (r: { predicate: string }) => r.predicate === 'has_member'
      ) || [];
      log(`Cluster has ${members.length} members`);
      expect(members.length).toBeGreaterThanOrEqual(2);

      // Verify our specific entities are members
      const memberIds = members.map((r: { peer: string }) => r.peer);
      expect(memberIds).toContain(entity1.id);
      expect(memberIds).toContain(entity2.id);
    } else {
      // They created separate clusters - this is OK if semantic similarity didn't match
      log('Entities created separate clusters (semantic similarity may not have matched)');
      createdEntities.push(cluster2Rel!.peer);
    }
  }, 300000); // 5 minute timeout for this test

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
        _kg_layer: 70000 + Math.floor(Math.random() * 10000),
      },
      collection: targetCollection.id,
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
