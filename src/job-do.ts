/**
 * KladosJobDO - Durable Object for clustering job processing
 *
 * This DO handles persistence and alarm scheduling for long-running jobs.
 * All lifecycle logic (logging, handoffs, provenance) is delegated to
 * KladosJob from @arke-institute/rhiza.
 */

import { DurableObject } from 'cloudflare:workers';
import {
  KladosJob,
  generateId,
  type KladosRequest,
  type KladosJobConfig,
} from '@arke-institute/rhiza';
import { processJob } from './job';
import type { Env } from './types';

/**
 * Job status state machine
 */
type JobStatus = 'accepted' | 'processing' | 'done' | 'error';

/**
 * KladosJobDO - Durable Object that processes clustering jobs
 *
 * Each job gets its own DO instance (keyed by job_id).
 * Processing happens in alarm handler, allowing multi-minute operations.
 */
export class KladosJobDO extends DurableObject<Env> {
  private sql: SqlStorage;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.initSchema();
  }

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

  private async handleStart(request: Request): Promise<Response> {
    const body = await request.json() as {
      request: KladosRequest;
      config: KladosJobConfig;
    };
    const { request: kladosRequest, config } = body;

    // Check if already started (idempotency)
    const existing = this.sql.exec('SELECT status FROM job_state WHERE id = 1').toArray()[0];
    if (existing) {
      return Response.json({
        accepted: true,
        job_id: kladosRequest.job_id,
      });
    }

    const logId = `log_${generateId()}`;
    const now = new Date().toISOString();
    this.sql.exec(
      `INSERT INTO job_state (id, request, config, log_id, status, created_at)
       VALUES (1, ?, ?, ?, 'accepted', ?)`,
      JSON.stringify(kladosRequest),
      JSON.stringify(config),
      logId,
      now
    );

    await this.ctx.storage.setAlarm(Date.now() + 100);

    return Response.json({
      accepted: true,
      job_id: kladosRequest.job_id,
    });
  }

  private handleStatus(): Response {
    const row = this.sql.exec('SELECT status, error FROM job_state WHERE id = 1').toArray()[0];
    if (!row) {
      return Response.json({ status: 'not_found' }, { status: 404 });
    }

    return Response.json({
      status: row.status,
      error: row.error,
    });
  }

  /**
   * Alarm handler - processes the job
   *
   * Delegates all lifecycle logic (logging, handoffs, provenance) to KladosJob.
   * Only handles persistence (SQL) and rescheduling (alarms).
   */
  async alarm(): Promise<void> {
    const row = this.sql.exec('SELECT * FROM job_state WHERE id = 1').toArray()[0];
    if (!row) return;

    const status = row.status as JobStatus;
    if (status === 'done' || status === 'error') return;

    this.sql.exec(`UPDATE job_state SET status = 'processing' WHERE id = 1`);

    const request: KladosRequest = JSON.parse(row.request as string);
    const config: KladosJobConfig = JSON.parse(row.config as string);

    const job = KladosJob.accept(request, config, undefined, {
      logFileId: (row.log_file_id as string) || undefined,
    });

    try {
      await job.start();

      if (job.logEntityId && !row.log_file_id) {
        this.sql.exec(`UPDATE job_state SET log_file_id = ? WHERE id = 1`, job.logEntityId);
      }

      const result = await processJob({ job, sql: this.sql, env: this.env });

      if (result.reschedule) {
        job.log.info('Rescheduling for continued processing');
        await this.ctx.storage.setAlarm(Date.now() + 1000);
        return;
      }

      await job.complete(result.outputs || []);
      this.sql.exec(`UPDATE job_state SET status = 'done' WHERE id = 1`);
    } catch (error) {
      await job.fail(error);
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.sql.exec(
        `UPDATE job_state SET status = 'error', error = ? WHERE id = 1`,
        errorMessage
      );
    }
  }
}
