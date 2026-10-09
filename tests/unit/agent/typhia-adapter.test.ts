import { describe, expect, it } from 'vitest';
import { createServer, type Server } from 'node:http';
import { TyphiaAdapter } from '../../../src/agent/typhia/adapter';
import type { AgentEvent } from '../../../src/agent/types';

// TyphiaAdapter 合同（SDD typhia-sdd-bridge-fork §3.2）：SSE→AsyncIterable、终态收口、
// stop()→/agent/stop、ctx 结构化透传、鉴权头。mock daemon 只讲 daemon 端点合同。

describe('TyphiaAdapter', () => {
  let srv: Server;
  let lastRunBody: any;
  let lastStopBody: any;
  let lastHeaders: Record<string, string | string[] | undefined>;
  let stopHandler: (() => void) | undefined;

  async function startDaemon(): Promise<number> {
    srv = createServer((req, res) => {
      lastHeaders = req.headers;
      const chunks: string[] = [];
      req.on('data', (c) => chunks.push(String(c)));
      req.on('end', () => {
        if (req.url === '/agent/run') {
          lastRunBody = JSON.parse(chunks.join('') || '{}');
          res.writeHead(200, { 'content-type': 'text/event-stream' });
          const events: AgentEvent[] = [
            { type: 'system', sessionId: 'sess-1' },
            { type: 'text', delta: 'hello ' },
            { type: 'text', delta: 'world' },
            { type: 'done', sessionId: 'sess-1', terminationReason: 'normal' },
          ];
          for (const e of events) res.write(`data: ${JSON.stringify(e)}\n\n`);
          res.end();
        } else if (req.url === '/agent/stop') {
          lastStopBody = JSON.parse(chunks.join('') || '{}');
          res.writeHead(200, { 'content-type': 'application/json' });
          res.end('{"ok":true}');
          stopHandler?.();
        } else if (req.url === '/') {
          res.writeHead(200, { 'content-type': 'application/json' });
          res.end('{"service":"Antry"}');
        } else {
          res.writeHead(404);
          res.end();
        }
      });
    });
    await new Promise<void>((r) => srv.listen(0, '127.0.0.1', r));
    return (srv.address() as { port: number }).port;
  }

  it('streams SSE frames as AgentEvents and threads ctx/token', async () => {
    const port = await startDaemon();
    try {
      const adapter = new TyphiaAdapter({ daemonUrl: `http://127.0.0.1:${port}`, token: 't-1' });
      expect(await adapter.isAvailable()).toBe(true);
      const run = adapter.run({
        runId: 'r1',
        prompt: 'hi',
        sessionId: 'sess-0',
        ctx: { chatId: 'oc_1', senderId: 'ou_1', chatMode: 'group', scopeId: 'oc_1', source: 'im' },
      });
      const got: AgentEvent[] = [];
      for await (const e of run.events) got.push(e);
      expect(got.map((e) => e.type)).toEqual(['system', 'text', 'text', 'done']);
      expect((got[3] as any).sessionId).toBe('sess-1');
      expect(lastRunBody.prompt).toBe('hi');
      expect(lastRunBody.sessionId).toBe('sess-0');
      expect(lastRunBody.ctx).toEqual({ chatId: 'oc_1', senderId: 'ou_1', chatMode: 'group', scopeId: 'oc_1', source: 'im' });
      expect(lastHeaders['x-agent-token']).toBe('t-1');
      expect(await run.waitForExit(1000)).toBe(true);
    } finally {
      srv.close();
    }
  });

  it('emits a terminal error event when the daemon rejects the run', async () => {
    const port = await startDaemon();
    try {
      const adapter = new TyphiaAdapter({ daemonUrl: `http://127.0.0.1:${port}/badbase` });
      const run = adapter.run({ runId: 'r2', prompt: 'x' });
      const got: AgentEvent[] = [];
      for await (const e of run.events) got.push(e);
      const last = got[got.length - 1];
      expect(Boolean(last && last.type === 'error' && last.terminationReason === 'failed')).toBe(true);
    } finally {
      srv.close();
    }
  });

  it('stop() posts /agent/stop with chat identity', async () => {
    const port = await startDaemon();
    try {
      const adapter = new TyphiaAdapter({ daemonUrl: `http://127.0.0.1:${port}` });
      const run = adapter.run({
        runId: 'r3',
        prompt: 'x',
        ctx: { chatId: 'oc_9', senderId: 'ou_9', scopeId: 'oc_9', source: 'im' },
      });
      await run.stop();
      expect(lastStopBody.ctx).toEqual({ chatId: 'oc_9', senderId: 'ou_9' });
      // 消费侧收到 aborted 后的 error 终态，不悬流
      const got: AgentEvent[] = [];
      for await (const e of run.events) got.push(e);
      expect(got.length).toBeGreaterThan(0);
    } finally {
      srv.close();
    }
  });
});
