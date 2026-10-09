import type { AgentAdapter, AgentEvent, AgentRun, AgentRunOptions, AgentRunContext } from '../types';
import { AgentPreflightError, type AgentAvailability } from '../preflight';

/**
 * Typhia backend adapter (yibi-studio fork delta).
 *
 * Instead of spawning a local claude/codex CLI process, forwards runs to a
 * long-lived typhia daemon over HTTP: POST /agent/run returns an SSE stream of
 * AgentEvent frames, POST /agent/stop aborts the active turn for a chat.
 *
 * Configuration (env only, no profile-schema surface): TYPHIA_DAEMON_URL
 * (default http://127.0.0.1:8777), TYPHIA_AGENT_TOKEN (optional shared token,
 * sent as x-agent-token). The daemon panel binds 127.0.0.1 — same host only.
 *
 * This adapter intentionally injects nothing into the prompt: identity/chat
 * context travels structurally via AgentRunOptions.ctx.
 */
export interface TyphiaAdapterOptions {
  daemonUrl?: string;
  token?: string;
  fetchImpl?: typeof fetch;
}

export class TyphiaAdapter implements AgentAdapter {
  readonly id = 'typhia';
  readonly displayName = 'Typhia';

  private readonly daemonUrl: string;
  private readonly token?: string;
  private readonly fetchImpl: typeof fetch;

  constructor(opts: TyphiaAdapterOptions = {}) {
    this.daemonUrl = (
      opts.daemonUrl ??
      process.env.TYPHIA_DAEMON_URL ??
      'http://127.0.0.1:8777'
    ).replace(/\/+$/, '');
    this.token = opts.token ?? process.env.TYPHIA_AGENT_TOKEN;
    this.fetchImpl = opts.fetchImpl ?? fetch;
  }

  async isAvailable(): Promise<boolean> {
    return (await this.checkAvailability()).ok;
  }

  async checkAvailability(): Promise<AgentAvailability> {
    try {
      const r = await this.fetchImpl(`${this.daemonUrl}/`, { signal: AbortSignal.timeout(3000) });
      if (r.ok) return { ok: true };
      const diagnostic = {
        code: 'agent-version-check-nonzero-exit' as const,
        agentId: 'claude' as const,
        agentName: this.displayName,
        command: this.daemonUrl,
        exitCode: r.status,
      };
      return { ok: false, error: new AgentPreflightError(diagnostic, `typhia daemon unhealthy: HTTP ${r.status}`), diagnostic };
    } catch (err) {
      const diagnostic = {
        code: 'agent-binary-not-found' as const,
        agentId: 'claude' as const,
        agentName: this.displayName,
        command: this.daemonUrl,
        errno: (err as NodeJS.ErrnoException)?.code,
      };
      return { ok: false, error: new AgentPreflightError(diagnostic, `typhia daemon unreachable: ${err instanceof Error ? err.message : String(err)}`), diagnostic };
    }
  }

  run(opts: AgentRunOptions): AgentRun {
    const fetchImpl = this.fetchImpl;
    const headers: Record<string, string> = {
      'content-type': 'application/json',
      ...(this.token ? { 'x-agent-token': this.token } : {}),
    };
    const controller = new AbortController();
    let settled = false;
    let settle: (exited: boolean) => void = () => {};
    const exited = new Promise<boolean>(res => {
      settle = v => {
        if (!settled) {
          settled = true;
          res(v);
        }
      };
    });

    const daemonUrl = this.daemonUrl;
    async function* stream(): AsyncGenerator<AgentEvent> {
      try {
        const res = await fetchImpl(`${daemonUrl}/agent/run`, {
          method: 'POST',
          headers,
          signal: controller.signal,
          body: JSON.stringify({
            runId: opts.runId,
            prompt: opts.prompt,
            rawText: opts.rawText,
            sessionId: opts.sessionId,
            images: opts.images,
            ctx: opts.ctx,
          }),
        });
        if (!res.ok || !res.body) {
          yield { type: 'error', message: `typhia daemon rejected run: HTTP ${res.status}`, terminationReason: 'failed' };
          settle(true);
          return;
        }
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buf = '';
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          buf += decoder.decode(value, { stream: true });
          let idx: number;
          while ((idx = buf.indexOf('\n\n')) >= 0) {
            const frame = buf.slice(0, idx);
            buf = buf.slice(idx + 2);
            for (const line of frame.split('\n')) {
              if (!line.startsWith('data: ')) continue;
              try {
                yield JSON.parse(line.slice(6)) as AgentEvent;
              } catch {
                // Malformed frame: skip rather than kill the run.
              }
            }
          }
        }
        settle(true);
      } catch (err) {
        yield {
          type: 'error',
          message: `typhia daemon stream failed: ${err instanceof Error ? err.message : String(err)}`,
          terminationReason: 'failed',
        };
        settle(true);
      }
    }

    return {
      runId: opts.runId,
      events: stream(),
      stop: async () => {
        controller.abort();
        try {
          await fetchImpl(`${this.daemonUrl}/agent/stop`, {
            method: 'POST',
            headers,
            body: JSON.stringify({
              ctx: { chatId: opts.ctx?.chatId ?? opts.ctx?.scopeId, senderId: opts.ctx?.senderId ?? 'bridge' },
            }),
            signal: AbortSignal.timeout(5000),
          });
        } catch {
          // Daemon unreachable: the SSE abort already cut the consumer side.
        }
      },
      waitForExit: async (timeoutMs: number) =>
        await Promise.race([
          exited,
          new Promise<boolean>(res => {
            const t = setTimeout(() => res(false), timeoutMs);
            t.unref?.();
          }),
        ]),
    };
  }
}

/** Forward a foreign card-action click to the daemon's original handler. */
export async function forwardCardAction(
  adapter: TyphiaAdapter,
  input: { value: unknown; chatId: string; senderId: string },
): Promise<{ ok: boolean; toast?: string }> {
  const token = (adapter as unknown as { token?: string }).token;
  const daemonUrl = (adapter as unknown as { daemonUrl: string }).daemonUrl;
  const fetchImpl = (adapter as unknown as { fetchImpl: typeof fetch }).fetchImpl;
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    ...(token ? { 'x-agent-token': token } : {}),
  };
  try {
    const r = await fetchImpl(`${daemonUrl}/agent/card-action`, {
      method: 'POST',
      headers,
      body: JSON.stringify({ value: input.value, chatId: input.chatId, senderId: input.senderId }),
      signal: AbortSignal.timeout(15000),
    });
    if (!r.ok) return { ok: false };
    const j = (await r.json().catch(() => ({}))) as { ok?: boolean; toast?: string };
    return { ok: j.ok !== false, toast: typeof j.toast === 'string' ? j.toast : undefined };
  } catch {
    return { ok: false };
  }
}

export type { AgentRunContext };
