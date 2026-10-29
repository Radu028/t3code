import type { ResumeSessionConfig, SessionConfig, SessionEvent } from "@github/copilot-sdk";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  EnvironmentId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import { AcpRegistrySettings } from "../settings.ts";
import {
  copilotSdkCliCandidates,
  type CopilotSdkClient,
  type CopilotSdkClientOptions,
  type CopilotSdkSession,
} from "./copilotSdk.ts";
import { makeCopilotSdkAdapterV2 } from "./copilotSdkAdapter.ts";

const testLayer = Layer.mergeAll(
  NodeServices.layer,
  IdAllocator.layer,
  McpProviderSessions.layer,
  TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
);
const INSTANCE_ID = ProviderInstanceId.make("acpRegistry_copilot");
const THREAD_ID = ThreadId.make("thread-copilot-sdk");
const settings = Schema.decodeSync(AcpRegistrySettings)({
  enabled: true,
  agentId: "github-copilot-cli",
});
const policy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "approval-required",
  interactionMode: "default",
  cwd: null,
});
const selection = (options: ModelSelection["options"] = []): ModelSelection => ({
  instanceId: INSTANCE_ID,
  model: "claude-sonnet-4.5",
  options,
});

// Only fields the projection reads; the rest of each event is runtime bookkeeping.
const event = { id: "event", parentId: null, timestamp: "2026-10-29T00:00:00.000Z" } as const;

interface Call {
  readonly method: string;
  readonly args: ReadonlyArray<unknown>;
}

const makeFakeCopilot = Effect.fnUntraced(function* () {
  const calls = yield* Queue.unbounded<Call>();
  const record = (method: string, ...args: ReadonlyArray<unknown>) =>
    Queue.offerUnsafe(calls, { method, args });
  let onEvent: SessionConfig["onEvent"];
  let finishRun: ((error?: Error) => void) | undefined;
  const launches: Array<CopilotSdkClientOptions> = [];
  const resumes: Array<{ readonly sessionId: string; readonly config: ResumeSessionConfig }> = [];
  const resumeFailures: Array<Error> = [];
  const session: CopilotSdkSession = {
    sessionId: "copilot-native-1",
    send: async (options) => {
      record("send", options);
      return "steer-message";
    },
    sendAndWait: (options, timeout) => {
      record("sendAndWait", options, timeout);
      return new Promise((resolve, reject) => {
        finishRun = (error) => (error === undefined ? resolve(undefined) : reject(error));
      });
    },
    abort: async () => {
      record("abort");
      finishRun?.(new Error("The requested run was aborted before completion."));
    },
    setModel: async (model, options) => {
      record("setModel", model, options);
    },
    disconnect: async () => {},
    rpc: {
      mode: {
        set: async (params) => {
          record("mode.set", params);
          return { status: "applied", modelChanged: false };
        },
      },
      permissions: {
        setMode: async (params) => {
          record("permissions.setMode", params);
          return { success: true, mode: params.mode };
        },
      },
    },
  };
  const client: CopilotSdkClient = {
    createSession: async (config) => {
      record("createSession", config);
      onEvent = config.onEvent;
      return session;
    },
    resumeSession: async (sessionId, config) => {
      record("resumeSession", sessionId);
      resumes.push({ sessionId, config });
      const failure = resumeFailures.shift();
      if (failure) throw failure;
      onEvent = config.onEvent;
      return session;
    },
    stop: async () => [],
    forceStop: async () => {},
  };
  return {
    launches,
    resumes,
    resumeFailures,
    makeClient: async (options: CopilotSdkClientOptions) => {
      launches.push(options);
      return client;
    },
    emit: (sdkEvent: SessionEvent) => Effect.sync(() => onEvent?.(sdkEvent)),
    finishRun: (error?: Error) => Effect.sync(() => finishRun?.(error)),
    takeCall: Effect.fnUntraced(function* (method: string) {
      while (true) {
        const call = yield* Queue.take(calls);
        if (call.method === method) return call;
      }
    }),
  };
});

const makeHarness = Effect.fnUntraced(function* (
  fake: Effect.Success<ReturnType<typeof makeFakeCopilot>>,
  existingProviderThread?: OrchestrationV2ProviderThread,
) {
  const adapter = yield* makeCopilotSdkAdapterV2({
    instanceId: INSTANCE_ID,
    settings,
    environment: { PATH: "/usr/bin" },
    resolveSpawn: () =>
      Effect.succeed({ command: "/opt/copilot/bin/copilot", args: ["--acp"], env: {} }),
    makeClient: fake.makeClient,
  });
  const runtime = yield* adapter.openSession({
    threadId: THREAD_ID,
    providerSessionId: ProviderSessionId.make("session-copilot"),
    modelSelection: selection(),
    runtimePolicy: policy,
  });
  const emitted = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  yield* runtime.events.pipe(
    Stream.runForEach((item) => Queue.offer(emitted, item)),
    Effect.forkScoped,
  );
  const providerThread = yield* runtime.ensureThread({
    threadId: THREAD_ID,
    modelSelection: selection(),
    runtimePolicy: policy,
    ...(existingProviderThread ? { existingProviderThread } : {}),
  });
  const takeRunningTurn = Effect.fnUntraced(function* () {
    while (true) {
      const next = yield* Queue.take(emitted);
      if (next.type === "provider_turn.updated" && next.providerTurn.status === "running") {
        return next.providerTurn;
      }
    }
  });
  const untilTerminal = Effect.fnUntraced(function* () {
    const seen: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
    while (true) {
      const next = yield* Queue.take(emitted);
      seen.push(next);
      if (next.type === "turn.terminal") return seen;
    }
  });
  return { runtime, providerThread, takeRunningTurn, untilTerminal };
});

const turnInput = Effect.fnUntraced(function* (
  providerThread: OrchestrationV2ProviderThread,
  options: ModelSelection["options"] = [],
  ordinal = 1,
): Effect.fn.Return<ProviderAdapter.ProviderAdapterV2TurnInput> {
  const now = yield* DateTime.now;
  return {
    appThread: {
      id: THREAD_ID,
      projectId: ProjectId.make("project-copilot"),
      title: "Copilot",
      createdBy: "user",
      creationSource: "web",
      providerInstanceId: INSTANCE_ID,
      modelSelection: selection(options),
      runtimeMode: policy.runtimeMode,
      interactionMode: policy.interactionMode,
      branch: null,
      worktreePath: null,
      activeProviderThreadId: providerThread.id,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: THREAD_ID },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    },
    threadId: THREAD_ID,
    runId: RunId.make(`run-${ordinal}`),
    runOrdinal: ordinal,
    providerTurnOrdinal: ordinal,
    attemptId: RunAttemptId.make(`attempt-${ordinal}`),
    rootNodeId: NodeId.make(`root-${ordinal}`),
    providerThread,
    message: {
      messageId: MessageId.make(`message-${ordinal}`),
      text: "Fix the build",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
    modelSelection: selection(options),
    runtimePolicy: policy,
  };
});

const turnItems = (events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) =>
  events.flatMap((item) => (item.type === "turn_item.updated" ? [item.turnItem] : []));

describe("CopilotSdkAdapterV2", () => {
  it("runs the Copilot binary behind npm's Windows shim without a shell", () => {
    const shim = "C:\\Users\\radu\\AppData\\Roaming\\npm\\copilot.cmd";
    assert.deepStrictEqual(
      copilotSdkCliCandidates({ command: shim, platform: "win32", architecture: "x64" }),
      [
        "C:\\Users\\radu\\AppData\\Roaming\\npm\\node_modules\\@github\\copilot\\node_modules\\@github\\copilot-win32-x64\\copilot.exe",
        "C:\\Users\\radu\\AppData\\Roaming\\npm\\node_modules\\@github\\copilot-win32-x64\\copilot.exe",
        "C:\\Users\\radu\\AppData\\Roaming\\npm\\node_modules\\@github\\copilot\\npm-loader.js",
      ],
    );
    const winget = "C:\\Program Files\\GitHub Copilot\\copilot.exe";
    assert.deepStrictEqual(
      copilotSdkCliCandidates({ command: winget, platform: "win32", architecture: "x64" }),
      [winget],
    );
  });

  it.effect("projects messages, reasoning and tools from root events only", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeCopilot();
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread));
      const sent = yield* fake.takeCall("sendAndWait");
      assert.deepStrictEqual(sent.args, [{ prompt: "Fix the build" }, 2_147_483_647]);

      for (const sdkEvent of [
        {
          ...event,
          type: "assistant.reasoning_delta",
          ephemeral: true,
          data: { reasoningId: "r1", deltaContent: "Check " },
        },
        {
          ...event,
          type: "assistant.reasoning",
          data: { reasoningId: "r1", content: "Check the build." },
        },
        {
          ...event,
          type: "assistant.message_delta",
          ephemeral: true,
          data: { messageId: "m1", deltaContent: "Running " },
        },
        {
          ...event,
          type: "assistant.message_delta",
          ephemeral: true,
          data: { messageId: "m1", deltaContent: "the build." },
        },
        {
          ...event,
          type: "assistant.message",
          data: { messageId: "m1", content: "Running the build." },
        },
        {
          ...event,
          type: "tool.execution_start",
          data: { toolCallId: "t1", toolName: "bash", arguments: { command: "pnpm build" } },
        },
        {
          ...event,
          type: "tool.execution_complete",
          data: {
            toolCallId: "t1",
            success: true,
            result: { content: "error TS2322" },
            shellExecution: { exitCode: 2 },
          },
        },
        {
          ...event,
          type: "tool.execution_start",
          data: { toolCallId: "t2", toolName: "edit", arguments: { path: "src/a.ts" } },
        },
        {
          ...event,
          type: "tool.execution_complete",
          data: {
            toolCallId: "t2",
            success: true,
            result: {
              content: "Edited",
              detailedContent: "--- a/src/a.ts\n+++ b/src/a.ts\n@@ -1 +1 @@\n-a\n+b\n",
            },
            fileEdits: [{ path: "src/a.ts", kind: "edit" }],
          },
        },
        // A subagent's text belongs to its own thread, not the root turn.
        {
          ...event,
          agentId: "child-1",
          type: "assistant.message",
          data: { messageId: "c1", content: "child text" },
        },
        {
          ...event,
          type: "assistant.message",
          data: { messageId: "m2", content: "", toolRequests: [] },
        },
      ] satisfies ReadonlyArray<SessionEvent>) {
        yield* fake.emit(sdkEvent);
      }
      yield* fake.finishRun();
      const items = turnItems(yield* harness.untilTerminal());

      const messages = items.filter((item) => item.type === "assistant_message");
      assert.deepStrictEqual(
        messages.map((item) => [item.text, item.streaming]),
        [
          ["Running ", true],
          ["Running the build.", false],
        ],
      );
      const reasoning = items.filter((item) => item.type === "reasoning").at(-1);
      assert.strictEqual(reasoning?.type === "reasoning" && reasoning.text, "Check the build.");
      const shell = items.filter((item) => item.type === "command_execution").at(-1);
      assert.deepInclude(shell, {
        status: "completed",
        input: "pnpm build",
        output: "error TS2322",
        exitCode: 2,
      });
      const edit = items.filter((item) => item.type === "file_change").at(-1);
      assert.deepInclude(edit, {
        status: "completed",
        fileName: "src/a.ts",
        changes: [{ operation: "edit", path: "src/a.ts" }],
        diffStr: "--- a/src/a.ts\n+++ b/src/a.ts\n@@ -1 +1 @@\n-a\n+b\n",
      });
      assert.isFalse(
        items.some((item) => item.type === "assistant_message" && item.text === "child text"),
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("reports a Copilot error as the turn's provider failure", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeCopilot();
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread));
      yield* fake.takeCall("sendAndWait");
      yield* fake.emit({
        ...event,
        type: "session.error",
        data: { errorType: "rate_limit", message: "Rate limit exceeded", statusCode: 429 },
      });
      yield* fake.finishRun(new Error("Rate limit exceeded"));
      const terminal = (yield* harness.untilTerminal()).at(-1);
      assert.strictEqual(terminal?.type, "turn.terminal");
      if (terminal?.type !== "turn.terminal" || terminal.status !== "failed") {
        return assert.fail("expected a failed turn");
      }
      assert.strictEqual(terminal.failure.class, "usage_limit");
      assert.strictEqual(terminal.failure.message, "Rate limit exceeded");
      assert.strictEqual(terminal.threadDisposition, "reusable");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("steers with immediate delivery and interrupts by aborting", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeCopilot();
      const harness = yield* makeHarness(fake);
      const input = yield* turnInput(harness.providerThread);
      yield* harness.runtime.startTurn(input);
      const turnId = (yield* harness.takeRunningTurn()).id;
      yield* fake.takeCall("sendAndWait");
      const running = harness.providerThread;
      yield* harness.runtime.steerTurn({
        threadId: THREAD_ID,
        runId: input.runId,
        providerThread: running,
        providerTurnId: turnId,
        message: { ...input.message, messageId: MessageId.make("steer"), text: "Also run lint" },
      });
      const steer = yield* fake.takeCall("send");
      assert.deepStrictEqual(steer.args[0], { prompt: "Also run lint", mode: "immediate" });

      yield* harness.runtime.interruptTurn({ providerThread: running, providerTurnId: turnId });
      yield* fake.takeCall("abort");
      const terminal = (yield* harness.untilTerminal()).at(-1);
      assert.deepInclude(terminal, { type: "turn.terminal", status: "interrupted" });
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("applies mode, allow-all, model and effort before the turn's message", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeCopilot();
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(
        yield* turnInput(harness.providerThread, [
          { id: "mode", value: "https://agentclientprotocol.com/protocol/session-modes#autopilot" },
          { id: "allow_all", value: true },
          { id: "reasoning_effort", value: "high" },
        ]),
      );
      assert.deepStrictEqual((yield* fake.takeCall("setModel")).args, [
        "claude-sonnet-4.5",
        { reasoningEffort: "high" },
      ]);
      assert.deepStrictEqual((yield* fake.takeCall("mode.set")).args, [{ mode: "autopilot" }]);
      assert.deepStrictEqual((yield* fake.takeCall("permissions.setMode")).args, [
        { mode: "allow-all" },
      ]);
      yield* fake.takeCall("sendAndWait");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("resumes the persisted Copilot session with T3's system message and MCP server", () =>
    Effect.gen(function* () {
      yield* (yield* McpProviderSessions.McpProviderSessions).set({
        environmentId: EnvironmentId.make("environment-copilot"),
        threadId: THREAD_ID,
        providerSessionId: "session-copilot",
        providerInstanceId: INSTANCE_ID,
        endpoint: "http://127.0.0.1:43210/mcp",
        authorizationHeader: "Bearer thread-token",
        browserToolsAvailable: false,
      });
      const now = yield* DateTime.now;
      const driver = ProviderDriverKind.make("acpRegistry");
      const saved: OrchestrationV2ProviderThread = {
        id: (yield* IdAllocator.IdAllocatorV2).derive.providerThread({
          driver,
          providerInstanceId: INSTANCE_ID,
          nativeThreadId: "copilot-native-1",
        }),
        driver,
        providerInstanceId: INSTANCE_ID,
        providerSessionId: ProviderSessionId.make("session-copilot"),
        appThreadId: THREAD_ID,
        ownerNodeId: null,
        nativeThreadRef: {
          driver,
          nativeId: "copilot-native-1",
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };

      const broken = yield* makeFakeCopilot();
      broken.resumeFailures.push(new Error("Session not found"));
      const failed = yield* makeHarness(broken, saved).pipe(Effect.result);
      assert.strictEqual(failed._tag, "Failure");

      const fake = yield* makeFakeCopilot();
      yield* makeHarness(fake, saved);
      assert.strictEqual(fake.resumes.length, 1);
      const { sessionId, config } = fake.resumes[0]!;
      assert.strictEqual(sessionId, "copilot-native-1");
      assert.deepStrictEqual(config.mcpServers, {
        "t3-code": {
          type: "http",
          url: "http://127.0.0.1:43210/mcp",
          headers: { Authorization: "Bearer thread-token" },
          tools: ["*"],
        },
      });
      assert.deepInclude(config.systemMessage, {
        mode: "customize",
        sections: { tone: { action: "remove" } },
      });
      assert.include(
        config.systemMessage?.content,
        "running in T3 Code through the GitHub Copilot harness",
      );
      assert.include(config.systemMessage?.content, "## T3 Code orchestration");
      assert.deepStrictEqual(
        fake.launches.map((launch) => launch.cliPath),
        ["/opt/copilot/bin/copilot"],
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
});
