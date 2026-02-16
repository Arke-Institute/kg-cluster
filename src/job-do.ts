/**
 * KladosJobDO - Durable Object for klados job processing
 *
 * Handles the full lifecycle of a clustering job:
 * - Stores job state in SQLite
 * - Writes initial log entry
 * - Processes via alarm (no 30s limit)
 * - Handles workflow handoffs
 */

import { DurableObject } from 'cloudflare:workers';
import { ArkeClient } from '@arke-institute/sdk';
import {
  KladosLogger,
  writeKladosLog,
  updateLogStatus,
  updateLogWithHandoffs,
  interpretThen,
  failKlados,
  generateId,
  type KladosRequest,
  type KladosLogEntry,
  type FlowStep,
} from '@arke-institute/rhiza';
import { processJob } from './job';
import type { Env } from './types';

/**
 * Job configuration passed from the worker
 */
export interface KladosJobConfig {
  agentId: string;
  agentVersion: string;
  authToken: string;
}

/**
 * Job status state machine
 */
type JobStatus = 'accepted' | 'processing' | 'done' | 'error';

/**
 * KladosJobDO - Durable Object that processes clustering jobs
 */
export class KladosJobDO extends DurableObject<Env> {
  private sql: SqlStorage;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.initSchema();
  }

  /**
   * Initialize SQLite schema for job state
   */
  private initSchema() {
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS job_state (
        id INTEGER PRIMARY KEY,
        request TEXT NOT NULL,
        config TEXT NOT NULL,
        log_id TEXT NOT NULL,
        log_file_id TEXT,
        status TEXT NOT NULL DEFAULT 'accepted',
        created_at TEXT NOT NULL,
        error TEXT
      );
    `);
  }

  /**
   * Handle incoming requests from the worker
   */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/start') {
      return this.handleStart(request);
    }

    if (url.pathname === '/status') {
      return this.handleStatus();
    }

    return new Response('Not found', { status: 404 });
  }

  /**
   * Start a new job
   */
  private async handleStart(request: Request): Promise<Response> {
    const body = await request.json() as {
      request: KladosRequest;
      config: KladosJobConfig;
    };
    const { request: kladosRequest, config } = body;

    // Check if already started (idempotency)
    const existing = this.sql.exec('SELECT status FROM job_state WHERE id = 1').toArray();
    if (existing.length > 0) {
      return Response.json({
        accepted: true,
        job_id: kladosRequest.job_id,
      });
    }

    // Generate log ID upfront
    const logId = `log_${generateId()}`;

    // Save initial state
    const now = new Date().toISOString();
    this.sql.exec(
      `INSERT INTO job_state (id, request, config, log_id, status, created_at)
       VALUES (1, ?, ?, ?, 'accepted', ?)`,
      JSON.stringify(kladosRequest),
      JSON.stringify(config),
      logId,
      now
    );

    // Schedule alarm for immediate processing
    await this.ctx.storage.setAlarm(Date.now() + 100);

    return Response.json({
      accepted: true,
      job_id: kladosRequest.job_id,
    });
  }

  /**
   * Get current job status
   */
  private handleStatus(): Response {
    const rows = this.sql.exec('SELECT status, error FROM job_state WHERE id = 1').toArray();
    if (rows.length === 0) {
      return Response.json({ status: 'not_found' }, { status: 404 });
    }

    const row = rows[0];
    return Response.json({
      status: row.status,
      error: row.error,
    });
  }

  /**
   * Alarm handler - processes the job
   */
  async alarm(): Promise<void> {
    const rows = this.sql.exec('SELECT * FROM job_state WHERE id = 1').toArray();
    if (rows.length === 0) return;
    const row = rows[0];

    const request: KladosRequest = JSON.parse(row.request as string);
    const config: KladosJobConfig = JSON.parse(row.config as string);
    const logId = row.log_id as string;

    // Skip if already terminal
    const status = row.status as JobStatus;
    if (status === 'done' || status === 'error') return;

    // Update status to processing
    this.sql.exec(`UPDATE job_state SET status = 'processing' WHERE id = 1`);

    // Create client
    const client = new ArkeClient({
      baseUrl: request.api_base,
      authToken: config.authToken,
      network: request.network,
    });

    const logger = new KladosLogger();
    let logFileId = row.log_file_id as string | null;

    try {
      if (!logFileId) {
        const logEntry: KladosLogEntry = {
          id: logId,
          type: 'klados_log',
          klados_id: config.agentId,
          rhiza_id: request.rhiza?.id,
          job_id: request.job_id,
          started_at: new Date().toISOString(),
          status: 'running',
          received: {
            target_entity: request.target_entity,
            target_entities: request.target_entities,
            target_collection: request.target_collection,
            from_logs: request.rhiza?.parent_logs,
            batch: request.rhiza?.batch,
            scatter_total: request.rhiza?.scatter_total,
          },
        };

        const { fileId } = await writeKladosLog({
          client,
          jobCollectionId: request.job_collection,
          entry: logEntry,
          messages: logger.getMessages(),
          agentId: config.agentId,
          agentVersion: config.agentVersion,
          relationshipUpdaterUrl: 'https://scatter-utility.arke.institute',
          authToken: config.authToken,
          apiBase: request.api_base,
          network: request.network,
        });

        logFileId = fileId;
        this.sql.exec(`UPDATE job_state SET log_file_id = ? WHERE id = 1`, logFileId);
      }

      // Process the job
      const result = await processJob({
        request,
        client,
        logger,
        sql: this.sql,
        env: this.env,
      });

      // Handle reschedule
      if (result.reschedule) {
        logger.info('Rescheduling for continued processing');
        await this.ctx.storage.setAlarm(Date.now() + 1000);
        return;
      }

      // Handle workflow handoff if in a workflow
      if (request.rhiza && result.outputs && result.outputs.length > 0) {
        const { data: rhizaEntity, error: rhizaError } = await client.api.GET('/entities/{id}', {
          params: { path: { id: request.rhiza.id } },
        });

        if (rhizaError || !rhizaEntity) {
          throw new Error(`Failed to fetch rhiza: ${request.rhiza.id}`);
        }

        const flow = rhizaEntity.properties.flow as Record<string, FlowStep>;
        const currentStepName = request.rhiza.path?.at(-1);

        if (currentStepName && flow) {
          const myStep = flow[currentStepName];
          if (myStep?.then) {
            const handoffResult = await interpretThen(
              myStep.then,
              {
                client,
                rhizaId: request.rhiza.id,
                kladosId: config.agentId,
                jobId: request.job_id,
                targetCollection: request.target_collection,
                jobCollectionId: request.job_collection,
                flow,
                outputs: result.outputs || [],
                fromLogId: logFileId,
                path: request.rhiza.path,
                apiBase: request.api_base,
                network: request.network,
                batchContext: request.rhiza.batch,
                authToken: config.authToken,
              }
            );

            if (handoffResult.handoffRecord) {
              await updateLogWithHandoffs(client, logFileId, [handoffResult.handoffRecord]);
            }

            logger.info(`Handoff: ${handoffResult.action}`, {
              target: handoffResult.target,
              targetType: handoffResult.targetType,
            });
          }
        }
      }

      // Finalize log
      logger.success('Job completed');
      await updateLogStatus(client, logFileId, 'done', {
        messages: logger.getMessages(),
      });

      this.sql.exec(`UPDATE job_state SET status = 'done' WHERE id = 1`);

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logger.error('Job failed', { error: errorMessage });

      if (logFileId) {
        await failKlados(client, {
          logFileId,
          batchContext: request.rhiza?.batch,
          error,
          messages: logger.getMessages(),
        });
      }

      this.sql.exec(
        `UPDATE job_state SET status = 'error', error = ? WHERE id = 1`,
        errorMessage
      );
    }
  }
}
