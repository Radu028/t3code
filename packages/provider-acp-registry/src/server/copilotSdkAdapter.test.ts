// @effect-diagnostics nodeBuiltinImport:off -- copilotClientProcess wraps a Node ChildProcess.
import * as NodeChildProcess from "node:child_process";
import * as NodeStream from "node:stream";
import {
  CopilotClient,
  RuntimeConnection,
  type ResumeSessionConfig,
  type SessionConfig,
  type SessionEvent,
} from "@github/copilot-sdk";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it, vi } from "@effect/vitest";
import {
  EnvironmentId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Sink from "effect/Sink";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as HostProcess from "@t3tools/shared/HostProcess";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import { AcpRegistrySettings } from "../settings.ts";
import {
  copilotClientProcess,
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

const COPILOT_MESSAGE_ID = "b6593215-0d4f-4c2e-9a51-3f0e7c2d1a44";
const isProtocolError = Schema.is(ProviderAdapter.ProviderAdapterProtocolError);

type CompactResult = Awaited<ReturnType<CopilotSdkSession["rpc"]["history"]["compact"]>>;

// Only fields the projection reads; the rest of each event is runtime bookkeeping.
const event = { id: "event", parentId: null, timestamp: "2026-10-29T00:00:00.000Z" } as const;

interface Call {
  readonly method: string;
  readonly args: ReadonlyArray<unknown>;
}

const makeFakeCopilot = Effect.fnUntraced(function* () {
  const calls = yield* Queue.unbounded<Call>();
  const history: Array<string> = [];
  const record = (method: string, ...args: ReadonlyArray<unknown>) => {
    history.push(method);
    Queue.offerUnsafe(calls, { method, args });
  };
  let config: SessionConfig | ResumeSessionConfig | undefined;
  let finishRun: ((error?: Error) => void) | undefined;
  let finishCompaction: ((result: CompactResult) => void) | undefined;
  const launches: Array<CopilotSdkClientOptions> = [];
  const resumes: Array<{ readonly sessionId: string; readonly config: ResumeSessionConfig }> = [];
  const resumeFailures: Array<Error> = [];
  let created = 0;
  let hangAbort = false;
  let stopFails = false;
  let stopErrors: Array<Error> = [];
  // After a stop() that reported errors the SDK no longer tracks its child.
  let childForgotten = false;
  // Each held list answers one tasks.list call, in order.
  const listGates: Array<Promise<void>> = [];
  let sendGate: Promise<void> | undefined;
  let rejectSend: ((cause: Error) => void) | undefined;
  let exitGate: Promise<void> | undefined;
  // Each held drain gates one waitForPending call, in order.
  const drainGates: Array<Promise<void>> = [];
  let runningTasks: Awaited<ReturnType<CopilotSdkSession["rpc"]["tasks"]["list"]>>["tasks"] = [];
  let failedLists = 0;
  const switchResults: Array<Awaited<ReturnType<CopilotSdkSession["rpc"]["model"]["switchTo"]>>> =
    [];
  const gate = () => {
    let release!: () => void;
    const promise = new Promise<void>((resolve) => {
      release = resolve;
    });
    return { promise, release: Effect.sync(() => release()) };
  };
  const makeSession = (sessionId: string): CopilotSdkSession => ({
    sessionId,
    send: async (options) => {
      record("send", options);
      // Like the SDK, forceStop disconnects and so rejects a send Copilot has not answered.
      await Promise.race([
        sendGate,
        new Promise<never>((_, reject) => {
          rejectSend = reject;
        }),
      ]);
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
      if (hangAbort) return new Promise<void>(() => {});
      finishRun?.(new Error("The requested run was aborted before completion."));
    },
    disconnect: async () => {
      record("disconnect", sessionId);
    },
    rpc: {
      model: {
        switchTo: async (params) => {
          record("model.switchTo", params);
          return switchResults.shift() ?? { modelId: params.modelId };
        },
      },
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
        handlePendingPermissionRequest: async (params) => {
          record("permissions.handlePendingPermissionRequest", params);
          // Copilot reports the outcome before it acknowledges the response.
          config?.onEvent?.({
            ...event,
            type: "permission.completed",
            data: {
              requestId: params.requestId,
              result: {
                kind: params.result.kind === "reject" ? "denied-interactively-by-user" : "approved",
              },
            },
          });
          return { success: true };
        },
      },
      tasks: {
        waitForPending: async () => {
          record("tasks.waitForPending");
          await drainGates.shift();
          return {};
        },
        list: async () => {
          record("tasks.list");
          await listGates.shift();
          if (failedLists > 0) {
            failedLists -= 1;
            throw new Error("tasks.list failed");
          }
          return { tasks: runningTasks };
        },
      },
      history: {
        compact: (params) => {
          record("history.compact", params);
          return new Promise((resolve) => {
            finishCompaction = resolve;
          });
        },
      },
    },
  });
  let exitProcess: () => void = () => {};
  let hasExitedNow: () => boolean = () => false;
  // Each launch is its own process, as relaunching the SDK client is.
  const makeClient = (): CopilotSdkClient => {
    let hasExited = false;
    hasExitedNow = () => hasExited;
    childForgotten = false;
    const exited = new Promise<void>((resolve) => {
      exitProcess = () => {
        hasExited = true;
        resolve();
      };
    });
    return {
      createSession: async (opened) => {
        record("createSession", opened);
        config = opened;
        return makeSession(`copilot-native-${++created}`);
      },
      resumeSession: async (sessionId, resumed) => {
        record("resumeSession", sessionId);
        resumes.push({ sessionId, config: resumed });
        const failure = resumeFailures.shift();
        if (failure) throw failure;
        config = resumed;
        return makeSession(sessionId);
      },
      stop: async () => {
        record("stop");
        if (stopErrors.length > 0) childForgotten = true;
        return stopErrors;
      },
      // Like the SDK: sessions disconnect, rejecting the request, before the process exits.
      forceStop: async () => {
        record("forceStop");
        finishRun?.(new Error("Session disconnected while waiting for a response"));
        rejectSend?.(new Error("Pending response rejected since connection got disposed"));
        if (stopFails) {
          throw new Error("Timed out waiting for CLI process to exit after SIGKILL: 10000ms");
        }
        if (childForgotten) return;
        await exitGate;
        exitProcess();
      },
      process: {
        id: 4242,
        hasExited: () => hasExited,
        exited,
        kill: () => {
          record("process.kill");
          if (!stopFails) exitProcess();
        },
      },
    };
  };
  return {
    launches,
    resumes,
    resumeFailures,
    makeClient: async (options: CopilotSdkClientOptions) => {
      launches.push(options);
      return makeClient();
    },
    called: (method: string) => history.includes(method),
    exitProcess: () => Effect.sync(() => exitProcess()),
    holdExit: () => {
      const held = gate();
      exitGate = held.promise;
      return held.release;
    },
    holdDrain: () => {
      const held = gate();
      drainGates.push(held.promise);
      return held.release;
    },
    /** The next waitForPending call never answers, though the connection stays open. */
    hangDrain: () => {
      drainGates.push(new Promise<void>(() => {}));
    },
    failTaskLists: (count: number) => {
      failedLists = count;
    },
    setRunningTasks: (count: number) => {
      runningTasks = Array.from({ length: count }, (_, index) => ({
        type: "shell" as const,
        id: `shell-${index}`,
        description: "pnpm test",
        status: "running" as const,
        startedAt: "2026-10-29T00:00:00.000Z",
        command: "pnpm test",
        attachmentMode: "attached" as const,
      }));
    },
    queueSwitchResult: (result: (typeof switchResults)[number]) => {
      switchResults.push(result);
    },
    config: () => config,
    hangAbort: () => {
      hangAbort = true;
    },
    /** SIGKILL does not end the CLI, so forceStop rejects and the process stays alive. */
    failForceStop: () => {
      stopFails = true;
    },
    /** stop() resolves with these errors, as the SDK does when the CLI ignores its kill. */
    failStop: (errors: Array<Error>) => {
      stopErrors = errors;
    },
    holdList: () => {
      const held = gate();
      listGates.push(held.promise);
      return held.release;
    },
    /** The next tasks.list call never answers, though the connection stays open. */
    hangList: () => {
      listGates.push(new Promise<void>(() => {}));
    },
    hasExited: () => hasExitedNow(),
    /** Copilot never acknowledges the next steer. */
    hangSend: () => {
      sendGate = new Promise<void>(() => {});
    },
    holdSend: () => {
      let release!: () => void;
      sendGate = new Promise((resolve) => {
        release = resolve;
      });
      return Effect.sync(() => release());
    },
    emit: (sdkEvent: SessionEvent) => Effect.sync(() => config?.onEvent?.(sdkEvent)),
    finishRun: (error?: Error) => Effect.sync(() => finishRun?.(error)),
    finishCompaction: (result: CompactResult) => Effect.sync(() => finishCompaction?.(result)),
    nextCall: Queue.take(calls),
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
  environment: NodeJS.ProcessEnv = { PATH: "/usr/bin" },
  instanceId = INSTANCE_ID,
) {
  const adapter = yield* makeCopilotSdkAdapterV2({
    instanceId,
    settings,
    environment,
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
  // Every write T3 would persist, in order: the adapter's events and T3's own saved answers.
  const writes: Array<
    | ProviderAdapter.ProviderAdapterV2Event
    | { readonly type: "t3.answered"; readonly requestId: string; readonly declined: boolean }
  > = [];
  yield* runtime.events.pipe(
    Stream.runForEach((item) => {
      writes.push(item);
      return Queue.offer(emitted, item);
    }),
    Effect.forkScoped,
  );
  // T3 saves an answer (request resolved, card settled) before it delivers it to the adapter.
  const respond = (input: ProviderAdapter.ProviderAdapterV2RuntimeRequestResponseInput) =>
    Effect.suspend(() => {
      const declined = input.decision === "decline" || input.decision === "cancel";
      writes.push({ type: "t3.answered", requestId: input.requestId, declined });
      return runtime.respondToRuntimeRequest(input);
    });
  /** A request's stored status and its card's: T3's projections upsert, so the last write wins. */
  const persisted = (requestId: string) => {
    let request: string | undefined;
    let card: string | undefined;
    for (const write of writes) {
      if (write.type === "t3.answered" && write.requestId === requestId) {
        request = "resolved";
        card = write.declined ? "cancelled" : "completed";
      }
      if (write.type === "runtime_request.updated" && write.runtimeRequest.id === requestId) {
        request = write.runtimeRequest.status;
      }
      if (
        write.type === "turn_item.updated" &&
        (write.turnItem.type === "approval_request" ||
          write.turnItem.type === "user_input_request") &&
        write.turnItem.requestId === requestId
      ) {
        card = write.turnItem.status;
      }
    }
    return { request, card };
  };
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
  const takeEvent = Effect.fnUntraced(function* (
    matches: (item: ProviderAdapter.ProviderAdapterV2Event) => boolean,
  ) {
    while (true) {
      const next = yield* Queue.take(emitted);
      if (matches(next)) return next;
    }
  });
  const until = Effect.fnUntraced(function* (
    matches: (item: ProviderAdapter.ProviderAdapterV2Event) => boolean,
  ) {
    const seen: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
    while (true) {
      const next = yield* Queue.take(emitted);
      seen.push(next);
      if (matches(next)) return seen;
    }
  });
  const untilTerminal = () => until((item) => item.type === "turn.terminal");
  return {
    runtime,
    providerThread,
    respond,
    persisted,
    takeRunningTurn,
    takeEvent,
    until,
    untilTerminal,
  };
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

/** A request's card, the adapter's last event when it opens one. */
const isCard = (requestId: string) => (item: ProviderAdapter.ProviderAdapterV2Event) =>
  item.type === "turn_item.updated" &&
  (item.turnItem.type === "approval_request" || item.turnItem.type === "user_input_request") &&
  item.turnItem.requestId === requestId;

const shellPermission = (requestId: string, command: string) =>
  ({
    ...event,
    type: "permission.requested",
    data: {
      requestId,
      permissionRequest: {
        kind: "shell",
        intention: "Run",
        fullCommandText: command,
        commands: [{ identifier: command, readOnly: false }],
        canOfferSessionApproval: false,
        hasWriteFileRedirection: false,
        possiblePaths: [],
        possibleUrls: [],
      },
    },
  }) satisfies SessionEvent;

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
      // An imported thread's provider thread and run-attempt ids are long derived ids.
      const importedThread = {
        ...harness.providerThread,
        id: ProviderThreadId.make(
          `provider-thread:provider:acpRegistry:provider-instance:acpRegistry_copilot:native-thread:${"2bc5779c-9d1e-4a6b-8f02-6c1d3e5a7b90".repeat(3)}`,
        ),
      };
      const attemptId = RunAttemptId.make(
        `run-attempt:run:run%3Athread%3Athread%253Aprovider%253A${"x".repeat(160)}:attempt:1`,
      );
      yield* harness.runtime.startTurn({ ...(yield* turnInput(importedThread)), attemptId });
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
          data: { messageId: COPILOT_MESSAGE_ID, deltaContent: "Running " },
        },
        {
          ...event,
          type: "assistant.message_delta",
          ephemeral: true,
          data: { messageId: COPILOT_MESSAGE_ID, deltaContent: "the build." },
        },
        {
          ...event,
          type: "assistant.message",
          data: { messageId: COPILOT_MESSAGE_ID, content: "Running the build." },
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
            // The model sees a truncated result; the row keeps the full output.
            result: {
              content: "error TS2322 [truncated]",
              detailedContent: "error TS2322\nerror TS2345",
            },
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
      // Ids come from Copilot's session and message ids, not T3's nested derived ids. A UUID
      // session gives 126 characters; AssistantCitation caps message ids at 512.
      for (const message of messages) {
        if (message.type !== "assistant_message") continue;
        assert.isAtMost(message.messageId.length, 160);
        assert.notInclude(message.messageId, "run-attempt");
      }
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
        output: "error TS2322\nerror TS2345",
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
      const events = yield* harness.untilTerminal();
      // T3 builds the error row from the terminal; a second one would duplicate it.
      assert.isFalse(turnItems(events).some((item) => item.type === "error"));
      const terminal = events.at(-1);
      assert.strictEqual(terminal?.type, "turn.terminal");
      if (terminal?.type !== "turn.terminal" || terminal.status !== "failed") {
        return assert.fail("expected a failed turn");
      }
      assert.strictEqual(terminal.failure.class, "usage_limit");
      assert.strictEqual(terminal.failure.message, "Rate limit exceeded");
      assert.strictEqual(terminal.threadDisposition, "reusable");

      // In autopilot a shell can outlive the failed request, so T3 stops Copilot first.
      fake.setRunningTasks(1);
      yield* harness.runtime.startTurn(
        yield* turnInput(
          harness.providerThread,
          [
            {
              id: "mode",
              value: "https://agentclientprotocol.com/protocol/session-modes#autopilot",
            },
          ],
          2,
        ),
      );
      yield* fake.takeCall("sendAndWait");
      yield* fake.emit({
        ...event,
        type: "tool.execution_start",
        data: { toolCallId: "edits", toolName: "powershell", arguments: { command: "./fix.ps1" } },
      });
      yield* fake.emit({
        ...event,
        type: "session.error",
        data: { errorType: "rate_limit", message: "Rate limit exceeded", statusCode: 429 },
      });
      yield* fake.finishRun(new Error("Rate limit exceeded"));
      const stopped = (yield* harness.untilTerminal()).at(-1);
      if (stopped?.type !== "turn.terminal" || stopped.status !== "failed") {
        return assert.fail("expected a failed turn");
      }
      assert.strictEqual(stopped.threadDisposition, "broken");
      assert.strictEqual(stopped.failure.class, "usage_limit");
      assert.isTrue(fake.called("forceStop"));

      // Nothing runs when T3 lists Copilot's tasks, but a steer Copilot took meanwhile may start
      // more work, so T3 stops Copilot here too.
      yield* harness.runtime.resumeThread({ providerThread: harness.providerThread });
      fake.setRunningTasks(0);
      const releaseList = fake.holdList();
      const failing = yield* turnInput(
        harness.providerThread,
        [{ id: "mode", value: "https://agentclientprotocol.com/protocol/session-modes#autopilot" }],
        3,
      );
      yield* harness.runtime.startTurn(failing);
      const failingTurn = (yield* harness.takeRunningTurn()).id;
      yield* fake.takeCall("sendAndWait");
      yield* fake.emit({
        ...event,
        type: "session.error",
        data: { errorType: "rate_limit", message: "Rate limit exceeded", statusCode: 429 },
      });
      yield* fake.finishRun(new Error("Rate limit exceeded"));
      yield* fake.takeCall("tasks.list");
      yield* harness.runtime.steerTurn({
        threadId: THREAD_ID,
        runId: failing.runId,
        providerThread: harness.providerThread,
        providerTurnId: failingTurn,
        message: { ...failing.message, messageId: MessageId.make("steer"), text: "Try again" },
      });
      yield* fake.emit({
        ...event,
        type: "user.message",
        data: { messageId: "steer-message", content: "Try again", delivery: "idle" },
      });
      yield* releaseList;
      assert.deepInclude((yield* harness.untilTerminal()).at(-1), {
        type: "turn.terminal",
        status: "failed",
        threadDisposition: "broken",
      });

      // A rate limit, then the CLI exits before the request settles: the turn keeps the usage
      // limit, and the process is still gone.
      yield* harness.runtime.resumeThread({ providerThread: harness.providerThread });
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, [], 4));
      yield* fake.takeCall("sendAndWait");
      yield* fake.emit({
        ...event,
        type: "session.error",
        data: { errorType: "rate_limit", message: "Rate limit exceeded", statusCode: 429 },
      });
      yield* fake.exitProcess();
      const exited = (yield* harness.untilTerminal()).at(-1);
      if (exited?.type !== "turn.terminal" || exited.status !== "failed") {
        return assert.fail("expected a failed turn");
      }
      assert.strictEqual(exited.threadDisposition, "broken");
      assert.strictEqual(exited.failure.class, "usage_limit");
      assert.strictEqual(exited.failure.message, "Rate limit exceeded");
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

      // A steer Copilot never acknowledges must not keep Stop from aborting, then killing.
      fake.hangSend();
      const second = yield* turnInput(running, [], 2);
      yield* harness.runtime.startTurn(second);
      const secondTurn = (yield* harness.takeRunningTurn()).id;
      yield* fake.takeCall("sendAndWait");
      const steering = yield* harness.runtime
        .steerTurn({
          threadId: THREAD_ID,
          runId: second.runId,
          providerThread: running,
          providerTurnId: secondTurn,
          message: { ...second.message, messageId: MessageId.make("steer-2"), text: "Faster" },
        })
        .pipe(Effect.forkScoped);
      yield* fake.takeCall("send");
      const stopping = yield* harness.runtime
        .interruptTurn({ providerThread: running, providerTurnId: secondTurn })
        .pipe(Effect.forkScoped);
      yield* fake.takeCall("abort");
      yield* TestClock.adjust("30 seconds");
      yield* Fiber.join(stopping);
      assert.deepInclude((yield* harness.untilTerminal()).at(-1), {
        type: "turn.terminal",
        status: "interrupted",
        threadDisposition: "broken",
      });
      assert.isTrue(Exit.isFailure(yield* Fiber.await(steering)));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("keeps the turn open for a steer Copilot runs after the request settles", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeCopilot();
      const harness = yield* makeHarness(fake);
      const input = yield* turnInput(harness.providerThread);
      yield* harness.runtime.startTurn(input);
      const turnId = (yield* harness.takeRunningTurn()).id;
      yield* fake.takeCall("sendAndWait");
      const releaseSend = fake.holdSend();
      const steering = yield* harness.runtime
        .steerTurn({
          threadId: THREAD_ID,
          runId: input.runId,
          providerThread: harness.providerThread,
          providerTurnId: turnId,
          message: { ...input.message, messageId: MessageId.make("steer"), text: "Also run lint" },
        })
        .pipe(Effect.forkScoped);
      yield* fake.takeCall("send");
      // The request ends while the steer is in flight, so Copilot runs it as a follow-up.
      yield* fake.emit({ ...event, type: "session.idle", ephemeral: true, data: {} });
      yield* fake.finishRun();
      yield* releaseSend;
      yield* Fiber.join(steering);
      for (const sdkEvent of [
        {
          ...event,
          type: "user.message",
          data: { messageId: "steer-message", content: "Also run lint" },
        },
        {
          ...event,
          type: "assistant.message",
          data: { messageId: "m-lint", content: "Lint passes" },
        },
        { ...event, type: "session.idle", ephemeral: true, data: { aborted: false } },
      ] satisfies ReadonlyArray<SessionEvent>) {
        yield* fake.emit(sdkEvent);
      }
      const events = yield* harness.untilTerminal();
      assert.isTrue(
        turnItems(events).some(
          (item) => item.type === "assistant_message" && item.text === "Lint passes",
        ),
      );
      assert.deepInclude(events.at(-1), { type: "turn.terminal", status: "completed" });
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("keeps the turn open until a steer's own run idles", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeCopilot();
      const harness = yield* makeHarness(fake);
      const input = yield* turnInput(harness.providerThread);
      yield* harness.runtime.startTurn(input);
      const turnId = (yield* harness.takeRunningTurn()).id;
      yield* fake.takeCall("sendAndWait");
      yield* harness.runtime.steerTurn({
        threadId: THREAD_ID,
        runId: input.runId,
        providerThread: harness.providerThread,
        providerTurnId: turnId,
        message: { ...input.message, messageId: MessageId.make("steer"), text: "Also run lint" },
      });
      // The request's run idles, then the steer starts a run of its own before the wait settles.
      for (const sdkEvent of [
        { ...event, type: "session.idle", ephemeral: true, data: { aborted: false } },
        {
          ...event,
          type: "user.message",
          data: { messageId: "steer-message", content: "Also run lint", delivery: "idle" },
        },
      ] satisfies ReadonlyArray<SessionEvent>) {
        yield* fake.emit(sdkEvent);
      }
      yield* fake.finishRun();
      // The SDK settles its promise before Copilot's next notifications arrive.
      yield* Effect.promise(() => Promise.resolve());
      for (const sdkEvent of [
        {
          ...event,
          type: "assistant.message",
          data: { messageId: "m-lint", content: "Lint passes" },
        },
        { ...event, type: "session.idle", ephemeral: true, data: { aborted: false } },
      ] satisfies ReadonlyArray<SessionEvent>) {
        yield* fake.emit(sdkEvent);
      }
      const events = yield* harness.untilTerminal();
      assert.isTrue(
        turnItems(events).some(
          (item) => item.type === "assistant_message" && item.text === "Lint passes",
        ),
      );
      assert.deepInclude(events.at(-1), { type: "turn.terminal", status: "completed" });
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("fails a steer's follow-up run on its error or on a CLI exit", () =>
    Effect.gen(function* () {
      // early-error: the follow-up fails before the request's own wait settles.
      for (const ending of ["error", "exit", "early-error"] as const) {
        yield* Effect.gen(function* () {
          const fake = yield* makeFakeCopilot();
          const harness = yield* makeHarness(fake);
          const input = yield* turnInput(harness.providerThread);
          yield* harness.runtime.startTurn(input);
          const turnId = (yield* harness.takeRunningTurn()).id;
          yield* fake.takeCall("sendAndWait");
          yield* harness.runtime.steerTurn({
            threadId: THREAD_ID,
            runId: input.runId,
            providerThread: harness.providerThread,
            providerTurnId: turnId,
            message: { ...input.message, messageId: MessageId.make("steer"), text: "Retry" },
          });
          const rateLimited = {
            ...event,
            type: "session.error",
            data: { errorType: "rate_limit", message: "Rate limit exceeded", statusCode: 429 },
          } satisfies SessionEvent;
          if (ending === "early-error") {
            for (const sdkEvent of [
              { ...event, type: "session.idle", ephemeral: true, data: { aborted: false } },
              {
                ...event,
                type: "user.message",
                data: { messageId: "steer-message", content: "Retry", delivery: "idle" },
              },
              rateLimited,
              { ...event, type: "session.idle", ephemeral: true, data: { aborted: false } },
            ] satisfies ReadonlyArray<SessionEvent>) {
              yield* fake.emit(sdkEvent);
            }
          }
          yield* fake.finishRun();
          yield* Effect.promise(() => Promise.resolve());
          if (ending !== "early-error") {
            yield* fake.emit({
              ...event,
              type: "user.message",
              data: { messageId: "steer-message", content: "Retry", delivery: "idle" },
            });
          }
          if (ending === "error") {
            yield* fake.emit(rateLimited);
          } else if (ending === "exit") {
            yield* fake.exitProcess();
          }
          const terminal = (yield* harness.untilTerminal()).at(-1);
          if (terminal?.type !== "turn.terminal" || terminal.status !== "failed") {
            return assert.fail(`expected a failed turn after the follow-up ${ending}`);
          }
          assert.strictEqual(
            terminal.failure.message,
            ending === "exit" ? "Copilot stopped unexpectedly." : "Rate limit exceeded",
          );
          assert.strictEqual(terminal.threadDisposition, ending === "exit" ? "broken" : "reusable");
        }).pipe(Effect.scoped);
      }
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("waits for Copilot's background work after an autopilot request", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeCopilot();
      const releaseFirstDrain = fake.holdDrain();
      const releaseSecondDrain = fake.holdDrain();
      const releaseThirdDrain = fake.holdDrain();
      const harness = yield* makeHarness(fake);
      const autopilot = [
        { id: "mode", value: "https://agentclientprotocol.com/protocol/session-modes#autopilot" },
      ];
      const input = yield* turnInput(harness.providerThread, autopilot);
      yield* harness.runtime.startTurn(input);
      const turnId = (yield* harness.takeRunningTurn()).id;
      yield* fake.takeCall("sendAndWait");
      yield* fake.emit({
        ...event,
        type: "tool.execution_start",
        data: { toolCallId: "bg", toolName: "bash", arguments: { command: "pnpm test" } },
      });
      // Autopilot completion does not wait for attached shells.
      yield* fake.finishRun();
      yield* fake.takeCall("tasks.waitForPending");
      // The drain returns on its own timeout while the shell still runs: not proof of completion.
      fake.setRunningTasks(1);
      yield* releaseFirstDrain;
      yield* fake.takeCall("tasks.list");
      yield* fake.emit({
        ...event,
        type: "tool.execution_complete",
        data: {
          toolCallId: "bg",
          success: true,
          result: { content: "ok" },
          shellExecution: { exitCode: 0 },
        },
      });
      fake.setRunningTasks(0);
      yield* fake.emit({
        ...event,
        type: "session.background_tasks_changed",
        ephemeral: true,
        data: {},
      });
      yield* fake.takeCall("tasks.waitForPending");
      // A shell starts while that drain runs, after Copilot already listed no running tasks.
      yield* fake.emit({
        ...event,
        type: "tool.execution_start",
        data: { toolCallId: "bg-late", toolName: "bash", arguments: { command: "pnpm build" } },
      });
      yield* fake.emit({
        ...event,
        type: "session.background_tasks_changed",
        ephemeral: true,
        data: {},
      });
      yield* releaseSecondDrain;
      const afterStaleList = yield* Effect.race(
        fake.takeCall("tasks.waitForPending").pipe(Effect.as("drained again")),
        harness.takeEvent((item) => item.type === "turn.terminal").pipe(Effect.as("ended")),
      );
      assert.strictEqual(afterStaleList, "drained again");
      // A steer during that drain starts its own run, which then decides the ending.
      yield* harness.runtime.steerTurn({
        threadId: THREAD_ID,
        runId: input.runId,
        providerThread: harness.providerThread,
        providerTurnId: turnId,
        message: { ...input.message, messageId: MessageId.make("steer"), text: "Also lint" },
      });
      yield* fake.emit({
        ...event,
        type: "user.message",
        data: { messageId: "steer-message", content: "Also lint", delivery: "idle" },
      });
      yield* releaseThirdDrain;
      yield* fake.emit({
        ...event,
        type: "tool.execution_complete",
        data: {
          toolCallId: "bg-late",
          success: true,
          result: { content: "built" },
          shellExecution: { exitCode: 0 },
        },
      });
      yield* fake.emit({
        ...event,
        type: "assistant.message",
        data: { messageId: "m-lint", content: "Lint passes" },
      });
      yield* fake.emit({
        ...event,
        type: "session.autopilot_stopped",
        ephemeral: true,
        data: { reason: "completed", requestId: "steer-message" },
      });
      const events = yield* harness.untilTerminal();
      assert.deepInclude(
        turnItems(events).findLast((item) => item.type === "command_execution"),
        { status: "completed", exitCode: 0 },
      );
      assert.isTrue(
        turnItems(events).some(
          (item) => item.type === "assistant_message" && item.text === "Lint passes",
        ),
      );
      assert.deepInclude(events.at(-1), { type: "turn.terminal", status: "completed" });

      // A task list Copilot cannot return gets a few fresh drains. A shell may still be running
      // then, so T3 stops Copilot before it takes the thread back. The first wait for background
      // work and the first list never answer.
      fake.hangDrain();
      fake.hangList();
      fake.failTaskLists(2);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, autopilot, 2));
      yield* fake.takeCall("sendAndWait");
      yield* fake.emit({
        ...event,
        type: "tool.execution_start",
        data: { toolCallId: "bg-unknown", toolName: "bash", arguments: { command: "pnpm dev" } },
      });
      yield* fake.finishRun();
      yield* fake.takeCall("tasks.waitForPending");
      yield* TestClock.adjust("11 minutes");
      yield* fake.takeCall("tasks.list");
      yield* TestClock.adjust("15 seconds");
      yield* Effect.all(Array.from({ length: 2 }, () => fake.takeCall("tasks.list")));
      const ended = (yield* harness.untilTerminal()).at(-1);
      if (ended?.type !== "turn.terminal") return assert.fail("no terminal");
      assert.deepInclude(ended, { status: "failed", threadDisposition: "broken" });
      assert.include(ended.failure?.message, "background work");
      assert.isTrue(fake.called("forceStop"));
      // The killed session takes no turn; T3 resumes the thread on a new process first.
      const next = yield* harness.runtime
        .startTurn(yield* turnInput(harness.providerThread, autopilot, 3))
        .pipe(Effect.result);
      assert.strictEqual(next._tag, "Failure");

      // A normal drain listed no tasks, then work started and the turn failed. That empty list,
      // answered late, must not settle the failure; only the failure's own list can.
      yield* harness.runtime.resumeThread({ providerThread: harness.providerThread });
      const releaseDrainList = fake.holdList();
      fake.hangList();
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, autopilot, 4));
      yield* fake.takeCall("sendAndWait");
      yield* fake.finishRun();
      yield* fake.takeCall("tasks.list");
      yield* fake.emit({
        ...event,
        type: "tool.execution_start",
        data: { toolCallId: "bg-watch", toolName: "bash", arguments: { command: "pnpm watch" } },
      });
      yield* fake.emit({
        ...event,
        type: "session.background_tasks_changed",
        ephemeral: true,
        data: {},
      });
      yield* fake.emit({
        ...event,
        type: "session.error",
        data: { errorType: "connection", message: "Connection lost" },
      });
      yield* fake.takeCall("tasks.list");
      yield* releaseDrainList;
      yield* TestClock.adjust("15 seconds");
      assert.deepInclude((yield* harness.untilTerminal()).at(-1), {
        type: "turn.terminal",
        status: "failed",
        threadDisposition: "broken",
      });
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("ends a policy-blocked steer through its warning and shows the warning", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeCopilot();
      const harness = yield* makeHarness(fake);
      const input = yield* turnInput(harness.providerThread);
      yield* harness.runtime.startTurn(input);
      const turnId = (yield* harness.takeRunningTurn()).id;
      yield* fake.takeCall("sendAndWait");
      yield* harness.runtime.steerTurn({
        threadId: THREAD_ID,
        runId: input.runId,
        providerThread: harness.providerThread,
        providerTurnId: turnId,
        message: { ...input.message, messageId: MessageId.make("steer"), text: "Push to main" },
      });
      yield* fake.finishRun();
      yield* Effect.promise(() => Promise.resolve());
      // Copilot emits no user.message for a policy-blocked prompt, only this warning.
      yield* fake.emit({
        ...event,
        type: "session.warning",
        data: {
          warningType: "policy_blocked",
          message: "Blocked by your organization's policy.",
          originatingMessageId: "steer-message",
        },
      });
      const events = yield* harness.untilTerminal();
      assert.deepInclude(
        turnItems(events).find((item) => item.type === "system_notice"),
        { message: "Blocked by your organization's policy." },
      );
      assert.deepInclude(events.at(-1), { type: "turn.terminal", status: "completed" });
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("keeps two instances' ids apart when they resume the same Copilot session", () =>
    Effect.gen(function* () {
      const ids = [];
      for (const instanceId of [INSTANCE_ID, ProviderInstanceId.make("acpRegistry_copilot_2")]) {
        ids.push(
          yield* Effect.gen(function* () {
            const fake = yield* makeFakeCopilot();
            const harness = yield* makeHarness(fake, undefined, undefined, instanceId);
            yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread));
            const providerTurn = yield* harness.takeRunningTurn();
            yield* fake.takeCall("sendAndWait");
            yield* fake.emit({
              ...event,
              type: "assistant.message",
              data: { messageId: COPILOT_MESSAGE_ID, content: "Hello" },
            });
            const message = yield* harness.takeEvent((item) => item.type === "message.updated");
            return [providerTurn.id, message.type === "message.updated" ? message.message.id : ""];
          }).pipe(Effect.scoped),
        );
      }
      const [first, second] = ids;
      assert.notStrictEqual(first?.[0], second?.[0]);
      assert.notStrictEqual(first?.[1], second?.[1]);
      for (const [, messageId] of ids) assert.isAtMost(messageId?.length ?? 0, 200);
    }).pipe(Effect.provide(testLayer)),
  );

  it.effect("relaunches Copilot and resumes its session after the CLI exits", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeCopilot();
      const sessionScope = yield* Scope.make();
      const harness = yield* makeHarness(fake).pipe(Scope.provide(sessionScope));
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread));
      yield* fake.takeCall("sendAndWait");
      yield* fake.finishRun(new Error("Session disconnected while waiting for a response"));
      assert.deepInclude((yield* harness.untilTerminal()).at(-1), {
        type: "turn.terminal",
        status: "failed",
        threadDisposition: "broken",
      });
      const resumed = yield* harness.runtime.resumeThread({
        providerThread: harness.providerThread,
      });
      assert.strictEqual(fake.launches.length, 2);
      assert.deepStrictEqual(
        fake.resumes.map((resume) => resume.sessionId),
        ["copilot-native-1"],
      );
      yield* harness.runtime.startTurn(yield* turnInput(resumed, [], 2));
      yield* fake.takeCall("sendAndWait");
      // stop() reports a CLI that ignored its kill, then forgets it; T3 kills the one it launched.
      fake.failStop([new Error("Timed out waiting for CLI process to exit after kill: 10000ms")]);
      yield* Scope.close(sessionScope, Exit.void);
      assert.isTrue(fake.called("process.kill"));
      assert.isTrue(fake.hasExited());
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("stops the Copilot process when an interrupt is never confirmed", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeCopilot();
      fake.hangAbort();
      const releaseExit = fake.holdExit();
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread));
      const turnId = (yield* harness.takeRunningTurn()).id;
      yield* fake.takeCall("sendAndWait");
      const interrupting = yield* harness.runtime
        .interruptTurn({ providerThread: harness.providerThread, providerTurnId: turnId })
        .pipe(Effect.forkScoped);
      yield* fake.takeCall("abort");
      yield* TestClock.adjust("30 seconds");
      // forceStop has rejected the request; the process has not exited yet.
      yield* fake.takeCall("forceStop");
      yield* releaseExit;
      yield* Fiber.join(interrupting);
      assert.deepInclude((yield* harness.untilTerminal()).at(-1), {
        type: "turn.terminal",
        status: "interrupted",
        threadDisposition: "broken",
      });
      yield* harness.runtime.resumeThread({ providerThread: harness.providerThread });
      assert.strictEqual(fake.launches.length, 2);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("kills Copilot's process tree on Windows and reports a failed kill", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeCopilot();
      fake.hangAbort();
      const spawned: Array<ReadonlyArray<string>> = [];
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.sync(() => {
          if ("args" in command) spawned.push([command.command, ...command.args]);
          return ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(1),
            exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
            isRunning: Effect.succeed(false),
            kill: () => Effect.void,
            unref: Effect.succeed(Effect.void),
            stdin: Sink.drain,
            stdout: Stream.empty,
            stderr: Stream.empty,
            all: Stream.empty,
            getInputFd: () => Sink.drain,
            getOutputFd: () => Stream.empty,
          });
        }),
      );
      const harness = yield* makeHarness(fake, undefined, {
        COPILOT_CLI_PATH: "C:\\tools\\copilot.exe",
      }).pipe(
        Effect.provideService(HostProcess.Platform, "win32"),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread));
      const turnId = (yield* harness.takeRunningTurn()).id;
      yield* fake.takeCall("sendAndWait");
      const interrupting = yield* harness.runtime
        .interruptTurn({ providerThread: harness.providerThread, providerTurnId: turnId })
        .pipe(Effect.forkScoped);
      yield* fake.takeCall("abort");
      yield* TestClock.adjust("30 seconds");
      yield* fake.takeCall("forceStop");
      assert.deepStrictEqual(spawned, [["taskkill", "/PID", "4242", "/T", "/F"]]);
      yield* harness.takeEvent(
        (item) =>
          item.type === "provider_session.updated" &&
          (item.providerSession.lastError ?? "").includes("taskkill exited 1"),
      );
      yield* Fiber.join(interrupting);
      // That process has exited; its pid may now be another process, so relaunch must not kill it.
      yield* harness.runtime.resumeThread({ providerThread: harness.providerThread });
      assert.strictEqual(fake.launches.length, 2);
      assert.strictEqual(spawned.length, 1);

      // This time the CLI outlives taskkill and SIGKILL. The turn still ends, but no second CLI
      // starts next to it.
      fake.failForceStop();
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, [], 2));
      const stuckTurn = (yield* harness.takeRunningTurn()).id;
      yield* fake.takeCall("sendAndWait");
      const interruptingStuck = yield* harness.runtime
        .interruptTurn({ providerThread: harness.providerThread, providerTurnId: stuckTurn })
        .pipe(Effect.forkScoped);
      yield* fake.takeCall("abort");
      yield* TestClock.adjust("30 seconds");
      yield* Fiber.join(interruptingStuck);
      const relaunch = yield* harness.runtime
        .resumeThread({ providerThread: harness.providerThread })
        .pipe(Effect.result);
      assert.strictEqual(relaunch._tag, "Failure");
      assert.strictEqual(fake.launches.length, 2);
      yield* harness.takeEvent(
        (item) =>
          item.type === "provider_session.updated" &&
          (item.providerSession.lastError ?? "").includes("is still running"),
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("replaces the native session when T3 forces a new one", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeCopilot();
      const harness = yield* makeHarness(fake);
      const replaced = yield* harness.runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: selection(),
        runtimePolicy: policy,
        existingProviderThread: { ...harness.providerThread, nativeThreadRef: null },
        forceNewNativeThread: true,
      });
      assert.strictEqual(replaced.id, harness.providerThread.id);
      assert.strictEqual(replaced.nativeThreadRef?.nativeId, "copilot-native-2");
      assert.deepStrictEqual((yield* fake.takeCall("disconnect")).args, ["copilot-native-1"]);
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
      assert.deepStrictEqual((yield* fake.takeCall("model.switchTo")).args, [
        { modelId: "claude-sonnet-4.5", reasoningEffort: "high", contextTier: "long_context" },
      ]);
      assert.deepStrictEqual((yield* fake.takeCall("mode.set")).args, [{ mode: "autopilot" }]);
      assert.deepStrictEqual((yield* fake.takeCall("permissions.setMode")).args, [
        { mode: "allow-all" },
      ]);
      yield* fake.takeCall("sendAndWait");
      yield* fake.finishRun();
      yield* harness.untilTerminal();
      // Dropping the effort option must clear Copilot's override, not keep "high".
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread, [], 2));
      assert.deepStrictEqual((yield* fake.takeCall("model.switchTo")).args, [
        { modelId: "claude-sonnet-4.5", reasoningEffort: null, contextTier: "long_context" },
      ]);
      yield* fake.takeCall("sendAndWait");
      yield* fake.finishRun();
      yield* harness.untilTerminal();
      // Copilot asks for compaction instead of switching: the turn must not run on the old model.
      fake.queueSwitchResult({
        confirmation: {
          targetModelDisplayName: "GPT-5.5",
          currentTokens: 180000,
          targetLimit: 128000,
        },
      });
      const smaller = yield* turnInput(harness.providerThread, [], 3);
      const gpt = { ...smaller, modelSelection: { ...smaller.modelSelection, model: "gpt-5.5" } };
      const refused = yield* harness.runtime.startTurn(gpt).pipe(Effect.flip);
      assert.include(
        isProtocolError(refused.cause) ? refused.cause.detail : undefined,
        "Send /compact",
      );
      const switchToGpt = [
        { modelId: "gpt-5.5", reasoningEffort: null, contextTier: "long_context" },
      ];
      assert.deepStrictEqual((yield* fake.takeCall("model.switchTo")).args, switchToGpt);
      // /compact runs on the current model, without retrying the switch it makes room for.
      const compactThread = harness.runtime.compactThread;
      if (!compactThread) return assert.fail("no compactThread");
      yield* compactThread({
        ...gpt,
        providerTurnOrdinal: 4,
        message: { ...gpt.message, text: "/compact" },
      });
      assert.deepInclude(yield* fake.nextCall, {
        method: "history.compact",
        args: [{ trigger: "manual" }],
      });
      yield* fake.emit({
        ...event,
        type: "session.compaction_start",
        data: { compactionAttemptId: "manual-1", currentTokens: 180000 },
      });
      // Copilot's result settles a compaction its events left open.
      yield* fake.finishCompaction({
        success: true,
        tokensRemoved: 140000,
        messagesRemoved: 30,
        summaryContent: "Fixed the build",
        contextWindow: { tokenLimit: 1000000, currentTokens: 40000, messagesLength: 4 },
      });
      const compacted = yield* harness.untilTerminal();
      assert.deepInclude(
        turnItems(compacted).findLast((item) => item.type === "compaction"),
        { status: "completed", beforeTokenCount: 180000, afterTokenCount: 40000 },
      );
      assert.deepInclude(compacted.at(-1), { type: "turn.terminal", status: "completed" });
      yield* harness.runtime.startTurn({ ...gpt, providerTurnOrdinal: 5 });
      assert.deepStrictEqual((yield* fake.takeCall("model.switchTo")).args, switchToGpt);
      yield* fake.takeCall("sendAndWait");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect(
    "opens Copilot sessions with T3's system message, MCP server, delegation, tier and discovery",
    () =>
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
        // With T3's MCP server the main agent delegates through delegate_task, on create and resume.
        // Discovery loads the user's skills as the CLI does, also on both.
        const delegation = {
          contextTier: "long_context",
          enableConfigDiscovery: true,
          defaultAgent: { excludedTools: ["task", "read_agent", "write_agent", "list_agents"] },
        } satisfies Partial<SessionConfig>;
        assert.deepInclude(config, delegation);
        const created = yield* makeFakeCopilot();
        yield* makeHarness(created);
        assert.deepInclude(created.config(), delegation);
        // Without it, Copilot keeps its own delegation tools.
        yield* (yield* McpProviderSessions.McpProviderSessions).clear(THREAD_ID);
        const native = yield* makeFakeCopilot();
        yield* makeHarness(native);
        assert.deepInclude(native.config(), {
          contextTier: "long_context",
          enableConfigDiscovery: true,
        });
        assert.notProperty(native.config(), "defaultAgent");
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
  it.effect("runs Copilot tasks as subagents with their own child threads", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeCopilot();
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread));
      yield* fake.takeCall("sendAndWait");
      const agent = { agentName: "explore", agentDisplayName: "Explore", agentDescription: "" };
      for (const sdkEvent of [
        {
          ...event,
          type: "tool.execution_start",
          data: {
            toolCallId: "task-sync",
            toolName: "task",
            arguments: { prompt: "Find the bug", description: "Find bug", agent_type: "explore" },
          },
        },
        {
          ...event,
          agentId: "agent-1",
          type: "subagent.started",
          data: { ...agent, toolCallId: "task-sync" },
        },
        {
          ...event,
          agentId: "agent-1",
          type: "assistant.message",
          data: { messageId: "c1", content: "Looking" },
        },
        {
          ...event,
          agentId: "agent-1",
          type: "tool.execution_start",
          data: { toolCallId: "c-t1", toolName: "grep", arguments: { pattern: "TODO" } },
        },
        {
          ...event,
          type: "tool.execution_start",
          data: {
            toolCallId: "task-bg",
            toolName: "task",
            arguments: { prompt: "Run tests", mode: "background", agent_type: "task" },
          },
        },
        {
          ...event,
          agentId: "agent-2",
          type: "subagent.started",
          data: { ...agent, toolCallId: "task-bg" },
        },
        // A background task's call settles at launch; the agent keeps running.
        {
          ...event,
          type: "tool.execution_complete",
          data: { toolCallId: "task-bg", success: true, result: { content: "Agent started" } },
        },
        {
          ...event,
          agentId: "agent-2",
          type: "assistant.message",
          data: { messageId: "c2", content: "Tests pass" },
        },
        {
          ...event,
          agentId: "agent-1",
          type: "subagent.completed",
          data: { ...agent, toolCallId: "task-sync", cancelled: true },
        },
        {
          ...event,
          agentId: "agent-2",
          type: "subagent.completed",
          data: { ...agent, toolCallId: "task-bg" },
        },
      ] satisfies ReadonlyArray<SessionEvent>) {
        yield* fake.emit(sdkEvent);
      }
      yield* fake.finishRun();
      const events = yield* harness.untilTerminal();
      const subagents = events.flatMap((item) =>
        item.type === "subagent.updated" ? [item.subagent] : [],
      );
      const last = (task: string) =>
        subagents.findLast((item) => item.nativeTaskRef?.nativeId === task);
      assert.deepInclude(last("task-sync"), { status: "cancelled", prompt: "Find the bug" });
      assert.deepInclude(last("task-bg"), { status: "completed", result: "Tests pass" });
      const childThreadId = last("task-sync")?.childThreadId;
      const childItems = turnItems(events).filter((item) => item.threadId === childThreadId);
      assert.deepStrictEqual(
        childItems.map((item) => [item.type, item.status]),
        [
          ["user_message", "completed"],
          ["assistant_message", "completed"],
          ["file_search", "running"],
          ["file_search", "cancelled"],
        ],
      );
      assert.isTrue(
        events.some(
          (item) => item.type === "app_thread.created" && item.appThread.id === childThreadId,
        ),
      );
      assert.isFalse(
        turnItems(events).some(
          (item) => item.threadId === THREAD_ID && item.type === "assistant_message",
        ),
      );
      assert.deepInclude(events.at(-1), { type: "turn.terminal", status: "completed" });
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("counts the main agent's usage once and tracks context and compaction", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeCopilot();
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread));
      yield* fake.takeCall("sendAndWait");
      const usage = (usageId: string, inputTokens: number) => ({
        model: "claude-sonnet-4.5",
        inputTokens,
        outputTokens: 10,
        cacheReadTokens: 5,
        accounting: { sequence: 1, sourceSessionId: "copilot-native-1", usageId },
      });
      for (const sdkEvent of [
        { ...event, type: "assistant.usage", ephemeral: true, data: usage("call-1", 100) },
        { ...event, type: "assistant.usage", ephemeral: true, data: usage("call-1", 100) },
        { ...event, type: "assistant.usage", ephemeral: true, data: usage("call-2", 50) },
        {
          ...event,
          agentId: "child",
          type: "assistant.usage",
          ephemeral: true,
          data: usage("child-1", 900),
        },
        {
          ...event,
          type: "session.usage_info",
          ephemeral: true,
          data: { currentTokens: 1200, tokenLimit: 200000, messagesLength: 4 },
        },
        {
          ...event,
          type: "session.compaction_start",
          data: { compactionAttemptId: "c1", currentTokens: 150000 },
        },
        {
          ...event,
          type: "session.compaction_complete",
          data: {
            compactionAttemptId: "c1",
            success: true,
            preCompactionTokens: 150000,
            postCompactionTokens: 20000,
            summaryContent: "Summary",
          },
        },
        // Copilot exits mid-compaction: its item must not stay running.
        {
          ...event,
          type: "session.compaction_start",
          data: { compactionAttemptId: "c2", currentTokens: 30000 },
        },
      ] satisfies ReadonlyArray<SessionEvent>) {
        yield* fake.emit(sdkEvent);
      }
      yield* fake.finishRun();
      const events = yield* harness.untilTerminal();
      const turn = events.findLast((item) => item.type === "provider_turn.updated");
      assert.deepInclude(
        turn?.type === "provider_turn.updated" ? turn.providerTurn.turnTokenUsage : undefined,
        {
          usageStatus: "complete",
          inputTokens: 150,
          outputTokens: 20,
          cachedInputTokens: 10,
        },
      );
      const context = events.findLast(
        (item) =>
          item.type === "provider_thread.updated" && item.providerThread.contextUsage !== null,
      );
      assert.deepStrictEqual(
        context?.type === "provider_thread.updated"
          ? context.providerThread.contextUsage
          : undefined,
        { usedTokens: 1200, maxTokens: 200000 },
      );
      const compaction = turnItems(events).filter((item) => item.type === "compaction");
      assert.deepStrictEqual(
        compaction.map(
          (item) =>
            item.type === "compaction" && [
              item.status,
              item.beforeTokenCount,
              item.afterTokenCount,
            ],
        ),
        [
          ["running", 150000, undefined],
          ["completed", 150000, 20000],
          ["running", 30000, undefined],
          ["failed", 30000, undefined],
        ],
      );
      assert.strictEqual(compaction.at(-1)?.title, "Context compaction failed");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("captures exit_plan_mode as a proposed plan and tells Copilot to stop", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeCopilot();
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread));
      yield* fake.takeCall("sendAndWait");
      const handler = fake.config()?.onExitPlanModeRequest;
      if (!handler) return assert.fail("exit_plan_mode handler missing");
      const answer = yield* Effect.promise(async () =>
        handler(
          {
            summary: "Plan",
            planContent: "1. Fix it",
            actions: ["autopilot"],
            recommendedAction: "autopilot",
          },
          { sessionId: "copilot-native-1" },
        ),
      );
      assert.strictEqual(answer.approved, false);
      const plan = yield* harness.takeEvent(
        (item) => item.type === "turn_item.updated" && item.turnItem.type === "proposed_plan",
      );
      assert.deepInclude(plan.type === "turn_item.updated" ? plan.turnItem : undefined, {
        markdown: "1. Fix it",
        status: "completed",
      });
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("routes approvals and questions to T3 runtime requests", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeCopilot();
      const harness = yield* makeHarness(fake);
      yield* harness.runtime.startTurn(yield* turnInput(harness.providerThread));
      yield* fake.takeCall("sendAndWait");
      yield* fake.emit({
        ...event,
        type: "permission.requested",
        data: {
          requestId: "perm-read",
          permissionRequest: { kind: "read", intention: "Read", path: "/etc/hosts" },
        },
      });
      assert.deepStrictEqual(
        (yield* fake.takeCall("permissions.handlePendingPermissionRequest")).args,
        [{ requestId: "perm-read", result: { kind: "approve-once" } }],
      );
      yield* fake.emit({
        ...event,
        agentId: "child",
        type: "permission.requested",
        data: {
          requestId: "perm-shell",
          permissionRequest: {
            kind: "shell",
            intention: "Install",
            fullCommandText: "pnpm install",
            commands: [{ identifier: "pnpm install", readOnly: false }],
            canOfferSessionApproval: true,
            hasWriteFileRedirection: false,
            possiblePaths: [],
            possibleUrls: [],
          },
        },
      });
      const shown = yield* harness.takeEvent((item) => item.type === "runtime_request.updated");
      if (shown.type !== "runtime_request.updated") return assert.fail("no request");
      assert.deepInclude(shown.runtimeRequest, { kind: "command", status: "pending" });
      assert.strictEqual(shown.runtimeRequest.nativeRequestRef?.nativeId, "perm-shell");
      const shell = shown.runtimeRequest.id;
      yield* harness.takeEvent(isCard(shell));
      yield* harness.respond({ requestId: shell, decision: "acceptForSession" });
      assert.deepStrictEqual(
        (yield* fake.takeCall("permissions.handlePendingPermissionRequest")).args,
        [
          {
            requestId: "perm-shell",
            result: {
              kind: "approve-for-session",
              approval: { kind: "commands", commandIdentifiers: ["pnpm install"] },
            },
          },
        ],
      );

      const ask = fake.config()?.onUserInputRequest;
      if (!ask) return assert.fail("ask_user handler missing");
      const answered = yield* Effect.promise(async () =>
        ask({ question: "Which one?", choices: ["A", "B"] }, { sessionId: "copilot-native-1" }),
      ).pipe(Effect.forkScoped);
      const afterShell = yield* harness.until(
        (item) =>
          item.type === "runtime_request.updated" && item.runtimeRequest.kind === "user_input",
      );
      const question = afterShell.at(-1);
      if (question?.type !== "runtime_request.updated") return assert.fail("no question");
      yield* harness.takeEvent(isCard(question.runtimeRequest.id));
      yield* harness.respond({ requestId: question.runtimeRequest.id, answers: { answer: "B" } });
      assert.deepStrictEqual(yield* Fiber.join(answered), { answer: "B", wasFreeform: false });

      // Cancelling a question still settles Copilot's callback, or Copilot keeps waiting.
      const declined = yield* Effect.promise(async () =>
        ask({ question: "Deploy now?" }, { sessionId: "copilot-native-1" }),
      ).pipe(Effect.forkScoped);
      const second = yield* harness.takeEvent(
        (item) =>
          item.type === "runtime_request.updated" &&
          item.runtimeRequest.kind === "user_input" &&
          item.runtimeRequest.id !== question.runtimeRequest.id,
      );
      if (second.type !== "runtime_request.updated") return assert.fail("no second question");
      yield* harness.takeEvent(isCard(second.runtimeRequest.id));
      yield* harness.respond({ requestId: second.runtimeRequest.id, decision: "cancel" });
      assert.deepStrictEqual(yield* Fiber.join(declined), {
        answer: "The user declined to answer.",
        wasFreeform: true,
      });

      // Copilot can stop waiting on an approval T3 never answered, and a turn can end on one.
      const opened = Effect.fnUntraced(function* (nativeId: string) {
        const request = yield* harness.takeEvent(
          (item) =>
            item.type === "runtime_request.updated" &&
            item.runtimeRequest.nativeRequestRef?.nativeId === nativeId,
        );
        return request.type === "runtime_request.updated" ? request.runtimeRequest.id : "";
      });
      yield* fake.emit(shellPermission("perm-gone", "pnpm publish"));
      const gone = yield* opened("perm-gone");
      yield* fake.emit({
        ...event,
        type: "permission.completed",
        data: { requestId: "perm-gone", result: { kind: "cancelled" } },
      });
      yield* fake.emit(shellPermission("perm-late", "pnpm deploy"));
      const late = yield* opened("perm-late");
      yield* fake.finishRun();
      yield* harness.untilTerminal();
      // Saved answers stay as T3 saved them; requests nobody answered close.
      assert.deepStrictEqual(
        [shell, question.runtimeRequest.id, second.runtimeRequest.id, gone, late].map(
          harness.persisted,
        ),
        [
          { request: "resolved", card: "completed" },
          { request: "resolved", card: "completed" },
          { request: "resolved", card: "cancelled" },
          { request: "cancelled", card: "cancelled" },
          { request: "cancelled", card: "cancelled" },
        ],
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it("reports Copilot's exit after its stdio closes or 5 seconds pass, then the SDK's drain", async () => {
    // A real SDK client on in-memory pipes; T3 reads its private CLI child and connection.
    const connect = async () => {
      const client = new CopilotClient({
        connection: RuntimeConnection.forStdio({ path: "copilot" }),
        useLoggedInUser: false,
      });
      const child = new NodeChildProcess.ChildProcess();
      Object.defineProperty(child, "pid", { value: 4242 });
      Object.assign(child, {
        stdin: new NodeStream.PassThrough(),
        stdout: new NodeStream.PassThrough(),
      });
      Reflect.set(client, "cliProcess", child);
      const connectViaStdio: unknown = Reflect.get(client, "connectToChildProcessViaStdio");
      if (typeof connectViaStdio !== "function") return assert.fail("The SDK cannot connect");
      await Reflect.apply(connectViaStdio, client, []);
      const tracked = copilotClientProcess(client);
      if (!tracked) return assert.fail("Copilot's process is not tracked");
      const exit = () => {
        Object.defineProperty(child, "exitCode", { value: 1 });
        child.emit("exit", 1, null);
      };
      // The SDK memoizes drain(): this is the barrier it awaits before it disconnects sessions.
      const drain = () => {
        const connection: unknown = Reflect.get(client, "connection");
        const sdkDrain: unknown =
          typeof connection === "object" && connection !== null
            ? Reflect.get(connection, "drain")
            : undefined;
        if (typeof sdkDrain !== "function") return assert.fail("The SDK connection has no drain");
        return Promise.resolve(Reflect.apply(sdkDrain, connection, []));
      };
      return { child, tracked, exit, drain };
    };

    const cli = await connect();
    assert.strictEqual(cli.tracked.id, 4242);
    assert.isFalse(cli.tracked.hasExited());
    cli.exit();
    assert.isTrue(cli.tracked.hasExited());
    cli.child.emit("close", 1, null);
    // Without the drain, the exit would show within microtasks, ahead of the barrier.
    const first = await Promise.race([
      cli.tracked.exited.then(() => "exited"),
      cli.drain().then(() => "drained"),
    ]);
    assert.strictEqual(first, "drained");

    // A grandchild can hold stdout open, so an exit without a close counts after 5 seconds.
    vi.useFakeTimers({ toFake: ["setTimeout"] });
    try {
      const orphaned = await connect();
      let exited = false;
      void orphaned.tracked.exited.then(() => {
        exited = true;
      });
      // Drained up front, so only the wait for stdio is left once Copilot exits.
      await orphaned.drain();
      orphaned.exit();
      await vi.advanceTimersByTimeAsync(4_999);
      assert.isFalse(exited);
      await vi.advanceTimersByTimeAsync(1);
      await orphaned.tracked.exited;
    } finally {
      vi.useRealTimers();
    }
  });
});
