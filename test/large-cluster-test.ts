/**
 * Large-Scale Clustering Test
 *
 * Creates 24 entities in 4 semantic groups and tests clustering behavior.
 * Entities are NOT cleaned up - they remain for frontend inspection.
 *
 * Usage:
 *   ARKE_USER_KEY=uk_... ARKE_NETWORK=main npx tsx test/large-cluster-test.ts
 */

import {
  configureTestClient,
  createCollection,
  createEntity,
  getEntity,
  invokeKlados,
  waitForKladosLog,
  log,
} from '@arke-institute/klados-testing';

// =============================================================================
// Configuration
// =============================================================================

const ARKE_API_BASE = process.env.ARKE_API_BASE || 'https://arke-v1.arke.institute';
const ARKE_USER_KEY = process.env.ARKE_USER_KEY;
const NETWORK = (process.env.ARKE_NETWORK || 'main') as 'test' | 'main';
const KLADOS_ID = process.env.KLADOS_ID || '01KJ60VSSCSQQ32PRJS0BQJ6Q8';

// Clustering parameters
const INDEX_WAIT_MS = 5000;
const FOLLOWER_WAIT_MIN_MS = 30000;
const FOLLOWER_WAIT_MAX_MS = 45000;
const POLL_INTERVAL_MS = 3000;

// =============================================================================
// Test Data: 4 Semantic Groups
// =============================================================================

interface TestEntity {
  label: string;
  description: string;
  group: string;
}

const TEST_ENTITIES: TestEntity[] = [
  // Group A: Whaling/Maritime (6 entities)
  { group: 'Whaling', label: 'Captain Ahab', description: 'Whaling ship captain obsessed with hunting the great white whale Moby Dick' },
  { group: 'Whaling', label: 'Ishmael', description: 'Narrator and sailor aboard the whaling ship Pequod who tells the tale' },
  { group: 'Whaling', label: 'The Pequod', description: 'Whaling vessel that sailed from New Bedford on its fateful voyage' },
  { group: 'Whaling', label: 'Moby Dick', description: 'The legendary great white whale hunted by Captain Ahab across the seas' },
  { group: 'Whaling', label: 'Queequeg', description: 'Skilled harpooner from the Pacific islands and Ishmael loyal companion' },
  { group: 'Whaling', label: 'Starbuck', description: 'First mate of the Pequod and voice of reason against Ahab obsession' },

  // Group B: Space Exploration (6 entities)
  { group: 'Space', label: 'Neil Armstrong', description: 'First astronaut to walk on the moon during the Apollo 11 mission in 1969' },
  { group: 'Space', label: 'Apollo 11', description: 'Historic NASA spacecraft that successfully landed humans on the lunar surface' },
  { group: 'Space', label: 'Buzz Aldrin', description: 'Lunar module pilot on Apollo 11 who walked on the moon with Armstrong' },
  { group: 'Space', label: 'Mission Control Houston', description: 'NASA command center that directed the Apollo space flights from Earth' },
  { group: 'Space', label: 'The Eagle', description: 'Lunar module spacecraft that touched down on the moon surface in 1969' },
  { group: 'Space', label: 'Michael Collins', description: 'Command module pilot who orbited the moon while others walked on surface' },

  // Group C: Ancient Greek Philosophy (6 entities)
  { group: 'Philosophy', label: 'Socrates', description: 'Ancient Greek philosopher known for the Socratic method of questioning' },
  { group: 'Philosophy', label: 'Plato', description: 'Student of Socrates who founded the Academy and wrote the Republic' },
  { group: 'Philosophy', label: 'Aristotle', description: 'Student of Plato and tutor to Alexander the Great who founded the Lyceum' },
  { group: 'Philosophy', label: 'The Academy', description: 'Philosophical school founded by Plato in ancient Athens for learning' },
  { group: 'Philosophy', label: 'The Lyceum', description: 'School of philosophy founded by Aristotle in Athens for his teachings' },
  { group: 'Philosophy', label: 'Diogenes', description: 'Cynic philosopher who lived in a barrel and rejected material possessions' },

  // Group D: Renaissance Art (6 entities)
  { group: 'Renaissance', label: 'Leonardo da Vinci', description: 'Renaissance genius who painted the Mona Lisa and The Last Supper' },
  { group: 'Renaissance', label: 'Michelangelo', description: 'Sculptor of David and painter of the Sistine Chapel ceiling masterpiece' },
  { group: 'Renaissance', label: 'Raphael', description: 'Renaissance master known for painting The School of Athens fresco' },
  { group: 'Renaissance', label: 'The Mona Lisa', description: 'Enigmatic portrait painted by Leonardo da Vinci now in the Louvre' },
  { group: 'Renaissance', label: 'The Sistine Chapel', description: 'Vatican chapel with ceiling painted by Michelangelo depicting Genesis' },
  { group: 'Renaissance', label: 'Botticelli', description: 'Florentine painter of The Birth of Venus and Primavera masterpieces' },
];

// =============================================================================
// Main Test
// =============================================================================

async function main() {
  if (!ARKE_USER_KEY) {
    console.error('ERROR: ARKE_USER_KEY environment variable required');
    process.exit(1);
  }

  log(`Starting large cluster test on ${NETWORK} network`);
  log(`API Base: ${ARKE_API_BASE}`);
  log(`Klados ID: ${KLADOS_ID}`);
  log(`Entities to create: ${TEST_ENTITIES.length}`);

  // Configure client
  configureTestClient({
    apiBase: ARKE_API_BASE,
    userKey: ARKE_USER_KEY,
    network: NETWORK,
  });

  // Use a unique layer for this test
  const testLayer = 50000 + Math.floor(Math.random() * 10000);
  log(`Using _kg_layer: ${testLayer}`);

  // ==========================================================================
  // Step 1: Create test collection
  // ==========================================================================
  log('\n=== Step 1: Creating test collection ===');
  const collection = await createCollection({
    label: `Large Cluster Test ${new Date().toISOString()}`,
    description: '24 entities in 4 semantic groups for clustering test',
    roles: { public: ['*:view', '*:invoke'] },
  });
  log(`Created collection: ${collection.id}`);

  // ==========================================================================
  // Step 2: Create all entities with sync_index
  // ==========================================================================
  log('\n=== Step 2: Creating 24 test entities ===');
  const createdEntities: Array<{ id: string; label: string; group: string }> = [];

  for (const entity of TEST_ENTITIES) {
    const response = await fetch(`${ARKE_API_BASE}/entities`, {
      method: 'POST',
      headers: {
        'Authorization': `ApiKey ${ARKE_USER_KEY}`,
        'X-Arke-Network': NETWORK,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        type: 'test_entity',
        collection: collection.id,
        properties: {
          label: entity.label,
          description: entity.description,
          _kg_layer: testLayer,
          _test_group: entity.group,
        },
        sync_index: true,
      }),
    });

    const created = await response.json();
    if (created.error) {
      log(`ERROR creating ${entity.label}: ${created.error}`);
      continue;
    }

    createdEntities.push({
      id: created.id,
      label: entity.label,
      group: entity.group,
    });
    log(`  [${entity.group}] ${entity.label}: ${created.id}`);
  }

  log(`\nCreated ${createdEntities.length} entities`);

  // ==========================================================================
  // Step 3: Wait for indexing
  // ==========================================================================
  log('\n=== Step 3: Waiting 45s for semantic index ===');
  await sleep(45000);

  // Test semantic search on a sample entity
  log('Testing semantic search...');
  const sampleEntity = createdEntities[0];
  const searchUrl = `${ARKE_API_BASE}/collections/${collection.id}/entities/search?similar_to=${sampleEntity.id}&limit=10`;
  const searchResponse = await fetch(searchUrl, {
    headers: {
      'Authorization': `ApiKey ${ARKE_USER_KEY}`,
      'X-Arke-Network': NETWORK,
    },
  });
  const searchResult = await searchResponse.json();
  log(`Similar to "${sampleEntity.label}": ${searchResult.entities?.length || 0} results`);
  for (const r of searchResult.entities?.slice(0, 5) || []) {
    log(`  - ${r.label} (score: ${r.score?.toFixed(3)})`);
  }

  // ==========================================================================
  // Step 4: Invoke clustering on all entities
  // ==========================================================================
  log('\n=== Step 4: Invoking clustering on all 24 entities ===');

  const invokePromises = createdEntities.map(async (entity) => {
    try {
      const result = await invokeKlados({
        kladosId: KLADOS_ID,
        targetEntity: entity.id,
        targetCollection: collection.id,
        input: {
          index_wait_ms: INDEX_WAIT_MS,
          initial_spread_ms: 0,
          follower_wait_min_ms: FOLLOWER_WAIT_MIN_MS,
          follower_wait_max_ms: FOLLOWER_WAIT_MAX_MS,
          follower_poll_interval_ms: POLL_INTERVAL_MS,
          k: 15,
          max_cluster_size: 5,
        },
        confirm: true,
      });
      return { entity, result, error: null };
    } catch (e) {
      return { entity, result: null, error: e };
    }
  });

  const invokeResults = await Promise.all(invokePromises);
  const successfulInvokes = invokeResults.filter(r => r.result?.status === 'started');
  log(`Successfully started ${successfulInvokes.length}/${createdEntities.length} jobs`);

  for (const { entity, result, error } of invokeResults) {
    if (error) {
      log(`  ERROR [${entity.group}] ${entity.label}: ${error}`);
    } else if (result?.status !== 'started') {
      log(`  FAILED [${entity.group}] ${entity.label}: ${result?.status}`);
    }
  }

  // ==========================================================================
  // Step 5: Wait for all jobs to complete
  // ==========================================================================
  log('\n=== Step 5: Waiting for jobs to complete (4 min timeout) ===');

  const completionPromises = successfulInvokes.map(async ({ entity, result }) => {
    try {
      const jobLog = await waitForKladosLog(result!.job_collection!, {
        timeout: 240000,
        pollInterval: 5000,
      });
      return { entity, log: jobLog, error: null };
    } catch (e) {
      return { entity, log: null, error: e };
    }
  });

  const completionResults = await Promise.all(completionPromises);
  const completed = completionResults.filter(r => r.log?.properties?.status === 'done');
  const failed = completionResults.filter(r => r.log?.properties?.status === 'error');
  const timedOut = completionResults.filter(r => !r.log);

  log(`\nCompletion results:`);
  log(`  Done: ${completed.length}`);
  log(`  Error: ${failed.length}`);
  log(`  Timed out: ${timedOut.length}`);

  // ==========================================================================
  // Step 6: Analyze cluster formation
  // ==========================================================================
  log('\n=== Step 6: Analyzing cluster formation ===');

  const clusterMap = new Map<string, Array<{ id: string; label: string; group: string }>>();
  const unclustered: Array<{ id: string; label: string; group: string }> = [];

  for (const entity of createdEntities) {
    try {
      const fullEntity = await getEntity(entity.id);
      const summarizedBy = fullEntity.relationships?.find(
        (r: { predicate: string }) => r.predicate === 'summarized_by'
      );

      if (summarizedBy) {
        const clusterId = summarizedBy.peer;
        if (!clusterMap.has(clusterId)) {
          clusterMap.set(clusterId, []);
        }
        clusterMap.get(clusterId)!.push(entity);
      } else {
        unclustered.push(entity);
      }
    } catch (e) {
      log(`  ERROR fetching ${entity.label}: ${e}`);
    }
  }

  // ==========================================================================
  // Step 7: Print report
  // ==========================================================================
  log('\n' + '='.repeat(60));
  log('CLUSTER FORMATION REPORT');
  log('='.repeat(60));

  log(`\nTotal entities: ${createdEntities.length}`);
  log(`Total clusters: ${clusterMap.size}`);
  log(`Unclustered entities: ${unclustered.length}`);

  log('\n--- Clusters ---');
  let clusterNum = 1;
  for (const [clusterId, members] of clusterMap) {
    const groupCounts = new Map<string, number>();
    for (const m of members) {
      groupCounts.set(m.group, (groupCounts.get(m.group) || 0) + 1);
    }
    const groupSummary = Array.from(groupCounts.entries())
      .map(([g, c]) => `${g}:${c}`)
      .join(', ');

    log(`\nCluster ${clusterNum} (${clusterId}):`);
    log(`  Members: ${members.length} [${groupSummary}]`);
    for (const m of members) {
      log(`    - [${m.group}] ${m.label}`);
    }
    clusterNum++;
  }

  if (unclustered.length > 0) {
    log('\n--- Unclustered (Singletons) ---');
    for (const e of unclustered) {
      log(`  - [${e.group}] ${e.label}`);
    }
  }

  // Check cross-group clustering
  log('\n--- Cross-Group Analysis ---');
  let pureGroupClusters = 0;
  let mixedClusters = 0;
  for (const [_, members] of clusterMap) {
    const groups = new Set(members.map(m => m.group));
    if (groups.size === 1) {
      pureGroupClusters++;
    } else {
      mixedClusters++;
    }
  }
  log(`Pure single-group clusters: ${pureGroupClusters}`);
  log(`Mixed cross-group clusters: ${mixedClusters}`);

  log('\n' + '='.repeat(60));
  log('TEST COMPLETE - Entities preserved for frontend inspection');
  log(`Collection ID: ${collection.id}`);
  log('='.repeat(60));
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Run the test
main().catch(e => {
  console.error('Test failed:', e);
  process.exit(1);
});
