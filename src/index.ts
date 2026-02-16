/**
 * KG Cluster Worker - Clusters semantically similar entities
 *
 * Tier 2 klados worker using Durable Objects for reliable processing.
 * Implements relationship-based discovery to find existing clusters.
 */

import { Hono } from 'hono';
import type { KladosRequest, KladosResponse } from '@arke-institute/rhiza';
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
  const req = await c.req.json<KladosRequest>();

  // Get DO instance by job_id (deterministic)
  const doId = c.env.KLADOS_JOB.idFromName(req.job_id);
  const doStub = c.env.KLADOS_JOB.get(doId);

  // Start the job in the DO
  const response = await doStub.fetch(
    new Request('https://do/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        request: req,
        config: {
          agentId: c.env.AGENT_ID,
          agentVersion: c.env.AGENT_VERSION,
          authToken: c.env.ARKE_AGENT_KEY,
        },
      }),
    })
  );

  return c.json(await response.json() as KladosResponse);
});

export { KladosJobDO };
export default app;
