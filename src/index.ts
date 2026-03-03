/**
 * KG Cluster Worker - Clusters semantically similar entities
 *
 * Tier 2 klados worker using Durable Objects for reliable processing.
 * Implements relationship-based discovery to find existing clusters.
 */

import { Hono } from 'hono';
import { getKladosConfig, type KladosRequest, type KladosResponse } from '@arke-institute/rhiza';
import { KladosJobDO } from './job-do';
import type { Env } from './types';

const app = new Hono<{ Bindings: Env }>();

/**
 * Health check endpoint
 */
app.get('/health', (c) => {
  return c.json({
    status: 'ok',
    agent_id: c.env.AGENT_ID,
    version: c.env.AGENT_VERSION,
    tier: 2,
  });
});

/**
 * Arke verification endpoint
 */
app.get('/.well-known/arke-verification', (c) => {
  const token = c.env.VERIFICATION_TOKEN;
  const kladosId = c.env.ARKE_VERIFY_AGENT_ID || c.env.AGENT_ID;

  if (!token || !kladosId) {
    return c.json({ error: 'Verification not configured' }, 500);
  }

  return c.json({
    verification_token: token,
    klados_id: kladosId,
  });
});

/**
 * Main job processing endpoint
 */
app.post('/process', async (c) => {
  console.log('[kg-cluster] /process endpoint called');
  const req = await c.req.json<KladosRequest>();
  console.log('[kg-cluster] job_id:', req.job_id, 'network:', req.network);

  // Get DO instance by job_id (deterministic)
  const doId = c.env.KLADOS_JOB.idFromName(req.job_id);
  const doStub = c.env.KLADOS_JOB.get(doId);
  console.log('[kg-cluster] DO stub obtained');

  // Get network-aware config for dual-network deployment
  const config = getKladosConfig(c.env, req.network);
  console.log('[kg-cluster] Config agentId:', config.agentId, 'authToken prefix:', config.authToken?.substring(0, 10));

  // Start the job in the DO
  const response = await doStub.fetch(
    new Request('https://do/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        request: req,
        config,
      }),
    })
  );
  console.log('[kg-cluster] DO response status:', response.status);

  const result = await response.json() as KladosResponse;
  console.log('[kg-cluster] Returning result:', JSON.stringify(result));
  return c.json(result);
});

export { KladosJobDO };
export default app;
