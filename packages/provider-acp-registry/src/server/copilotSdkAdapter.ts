import type {
  MessageOptions,
  PermissionHandler,
  SessionConfig,
  SessionEvent,
  SessionEventPayload,
} from "@github/copilot-sdk";
import {
  ProviderDriverKind,
  type ModelSelection,
  type NodeId,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2TurnItem,
  type OrchestrationV2TurnItemStatus,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { HTML_RENDER_TOOL_NAME } from "@t3tools/shared/htmlRender";
import {
  getModelSelectionBooleanOptionValue,
  getModelSelectionStringOptionValue,
} from "@t3tools/shared/model";
import { resolveT3McpToolId } from "@t3tools/shared/t3McpToolPresentation";
import { formatReadToolLabel, formatSearchToolLabel } from "@t3tools/shared/toolActivity";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import type { AcpSpawnInput } from "@t3tools/provider-acp/server/AcpSessionRuntime";
import { ACP_SESSION_MODE_OPTION_ID } from "@t3tools/provider-acp/server/sessionConfig";
import {
  isProviderNativeImageAttachment,
  providerMessageTextWithAttachmentPaths,
} from "@t3tools/provider-core/server/attachmentPrompt";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSession from "@t3tools/provider-core/server/mcpSession";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import { mcpToolPresentation } from "@t3tools/provider-core/server/mcpToolPresentation";
import {
  T3_CODE_BROWSER_TOOL_INSTRUCTIONS,
  t3OrchestrationSystemPrompt,
} from "@t3tools/provider-core/server/orchestrationInstructions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { buildRuntimeInstructions } from "@t3tools/provider-core/server/runtimeInstructions";
import { turnScopedSelectionTransition } from "@t3tools/provider-core/server/selectionTransition";
import type { AcpRegistrySettings } from "../settings.ts";
import type * as AcpRegistrySupport from "./AcpRegistrySupport.ts";
import {
  copilotSdkCliCandidates,
  makeCopilotSdkClient,
  type CopilotSdkClient,
  type CopilotSdkSession,
  type MakeCopilotSdkClient,
} from "./copilotSdk.ts";

// Copilot keeps the ACP Registry driver kind so released clients still render its threads.
const DRIVER = ProviderDriverKind.make("acpRegistry");

/** Node's setTimeout ceiling: a turn ends on Copilot's completion, never on a timer. */
const TURN_COMPLETION_TIMEOUT_MS = 2_147_483_647;
const INTERRUPT_CONFIRMATION_TIMEOUT: Duration.Input = "30 seconds";
const TEXT_FLUSH_INTERVAL: Duration.Input = "50 millis";

const CopilotSdkCapabilitiesV2 = {
  runtimePolicy: { enforcement: "client-boundary" },
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: true,
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: false,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: true,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: true,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true,
    streamsReasoning: true,
    streamsToolOutput: true,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: true,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: false,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: false,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: false,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: false,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: false,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: false,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: true,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: false,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "strong",
    nativeRequestIds: "weak",
  },
} satisfies OrchestrationV2ProviderCapabilities;

type CopilotMode = Parameters<CopilotSdkSession["rpc"]["mode"]["set"]>[0]["mode"];
type CopilotReasoningEffort = NonNullable<SessionConfig["reasoningEffort"]>;
type ToolCompletion = SessionEventPayload<"tool.execution_complete">["data"];
type CopilotError = SessionEventPayload<"session.error">["data"];

const REASONING_EFFORTS: ReadonlyArray<CopilotReasoningEffort> = [
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
];

/** What a turn needs set on the session: model, effort, Copilot's mode and allow-all. */
interface CopilotSessionSettings {
  readonly model: string;
  readonly reasoningEffort: CopilotReasoningEffort | undefined;
  readonly mode: CopilotMode;
  readonly allowAll: boolean;
}

/** Option ids are the ones the ACP probe advertises, so the model picker stays the same. */
function copilotSessionSettings(
  selection: ModelSelection,
  policy: ProviderAdapter.ProviderAdapterV2RuntimePolicy,
): CopilotSessionSettings {
  // Mode values are ACP URLs such as https://agentclientprotocol.com/protocol/session-modes#plan.
  const modeChoice = (
    getModelSelectionStringOptionValue(selection, "mode") ??
    getModelSelectionStringOptionValue(selection, ACP_SESSION_MODE_OPTION_ID)
  )
    ?.split("#")
    .pop();
  const effort = getModelSelectionStringOptionValue(selection, "reasoning_effort");
  const allowAllChoice =
    getModelSelectionBooleanOptionValue(selection, "allow_all") ??
    ["true", "on", "enabled"].includes(
      getModelSelectionStringOptionValue(selection, "allow_all") ?? "",
    );
  return {
    model: selection.model,
    reasoningEffort: REASONING_EFFORTS.find((candidate) => candidate === effort),
    mode:
      policy.interactionMode === "plan" || modeChoice === "plan"
        ? "plan"
        : modeChoice === "autopilot"
          ? "autopilot"
          : "interactive",
    allowAll: policy.runtimeMode === "full-access" || allowAllChoice,
  };
}

export interface CopilotSdkAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly settings: AcpRegistrySettings;
  readonly environment: NodeJS.ProcessEnv;
  /** Resolves the Copilot command the ACP Registry installed for this instance. */
  readonly resolveSpawn: (
    cwd: string,
  ) => Effect.Effect<AcpSpawnInput, AcpRegistrySupport.AcpRegistryError>;
  readonly makeClient?: MakeCopilotSdkClient;
}

interface TextSegment {
  readonly kind: "assistant_message" | "reasoning";
  text: string;
  streaming: boolean;
  emitted: boolean;
}

interface ToolCall {
  readonly toolCallId: string;
  readonly toolName: string;
  readonly title: string | undefined;
  readonly arguments: unknown;
  readonly mcpServerName: string | undefined;
  readonly mcpToolName: string | undefined;
  readonly displayCommand: string | undefined;
  completion?: ToolCompletion;
}

interface ActiveTurn {
  readonly input: ProviderAdapter.ProviderAdapterV2TurnInput;
  providerTurn: OrchestrationV2ProviderTurn;
  readonly nativeTurnId: string;
  readonly ordinals: Map<string, number>;
  readonly startedAt: Map<string, DateTime.Utc>;
  readonly texts: Map<string, TextSegment>;
  readonly tools: Map<string, ToolCall>;
  readonly dirty: Set<string>;
  readonly done: Deferred.Deferred<void>;
  flushScheduled: boolean;
  interruptRequested: boolean;
  lastError: CopilotError | undefined;
}

type Inbox =
  | { readonly type: "event"; readonly epoch: number; readonly event: SessionEvent }
  | { readonly type: "flush"; readonly turn: ActiveTurn }
  | {
      readonly type: "settled";
      readonly turn: ActiveTurn;
      readonly exit: Exit.Exit<unknown, ProviderAdapter.ProviderAdapterProtocolError>;
    };

// Copilot CLI's built-in tools, by the row T3 renders for them.
const SHELL_TOOLS = new Set(["bash", "powershell"]);
const EDIT_TOOLS = new Set(["edit", "create", "apply_patch", "str_replace_editor", "write"]);
const READ_TOOLS = new Set(["view", "read", "read_file"]);
const SEARCH_TOOLS = new Set(["grep", "glob", "rg", "search"]);
const FETCH_TOOLS = new Set(["web_fetch", "fetch"]);
// Copilot reports its status line through this tool; it is not work.
const HIDDEN_TOOLS = new Set(["report_intent"]);

const record = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;

const stringField = (value: unknown, ...keys: ReadonlyArray<string>): string | undefined => {
  const fields = record(value);
  for (const key of keys) {
    const field = fields?.[key];
    if (typeof field === "string" && field.trim().length > 0) return field.trim();
  }
  return undefined;
};

const looksLikeDiff = (text: string | undefined): text is string =>
  text !== undefined && /^(?:diff --git |--- |\+\+\+ |@@ )/m.test(text);

const nativeRef = (nativeId: string, strength: "strong" | "weak" = "strong") => ({
  driver: DRIVER,
  nativeId,
  strength,
});

const failureClass = (error: CopilotError | undefined) =>
  error?.statusCode === 429 || /rate.?limit|quota/i.test(error?.errorType ?? "")
    ? ("usage_limit" as const)
    : ("provider_error" as const);

const isProtocolError = Schema.is(ProviderAdapter.ProviderAdapterProtocolError);

const errorText = (cause: unknown) =>
  cause instanceof Error && cause.message.trim().length > 0 ? cause.message : undefined;

export const makeCopilotSdkAdapterV2 = Effect.fn("makeCopilotSdkAdapterV2")(function* (
  options: CopilotSdkAdapterV2Options,
) {
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const providerHost = yield* ProviderHost.ProviderHost;
  const fileSystem = yield* FileSystem.FileSystem;
  const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
  const platform = yield* HostProcess.Platform;
  const architecture = yield* HostProcess.Architecture;
  const makeClient = options.makeClient ?? makeCopilotSdkClient;

  const protocolError = (detail: string, cause?: unknown) =>
    new ProviderAdapter.ProviderAdapterProtocolError({
      driver: DRIVER,
      detail,
      ...(cause === undefined ? {} : { cause }),
    });

  const resolveCliPath = Effect.fnUntraced(function* (spawn: AcpSpawnInput) {
    const override = options.environment.COPILOT_CLI_PATH?.trim();
    if (override) return override;
    const candidates = copilotSdkCliCandidates({ command: spawn.command, platform, architecture });
    if (candidates.length === 1) return candidates[0]!;
    for (const candidate of candidates) {
      if (yield* fileSystem.exists(candidate).pipe(Effect.orElseSucceed(() => false))) {
        return candidate;
      }
    }
    return yield* protocolError(
      "Could not find the Copilot executable behind its npm shim; set COPILOT_CLI_PATH",
    );
  });

  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: DRIVER,
    getCapabilities: Effect.fn("CopilotSdkAdapter.getCapabilities")(function () {
      return Effect.succeed(CopilotSdkCapabilitiesV2);
    }),
    planSelectionTransition: Effect.fn("CopilotSdkAdapter.planSelectionTransition")(function () {
      return Effect.succeed(turnScopedSelectionTransition());
    }),
    openSession: Effect.fn("CopilotSdkAdapter.openSession")(function* (
      input: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
    ) {
      if (!options.settings.enabled) return yield* protocolError("Copilot is disabled");
      const scope = yield* Effect.scope;
      const requestedCwd = input.runtimePolicy.cwd ?? providerHost.paths.cwd;
      const cwd = yield* fileSystem
        .realPath(requestedCwd)
        .pipe(Effect.orElseSucceed(() => requestedCwd));
      const openError = (cause: unknown) =>
        new ProviderAdapter.ProviderAdapterOpenSessionError({
          driver: DRIVER,
          providerSessionId: input.providerSessionId,
          cause,
        });
      const spawn = yield* options.resolveSpawn(cwd).pipe(Effect.mapError(openError));
      const cliPath = yield* resolveCliPath(spawn).pipe(Effect.mapError(openError));
      const environment = McpProviderSession.withAgentDeviceEnvironment(
        { ...options.environment, ...spawn.env },
        yield* mcpSessions.read(input.threadId),
      );
      const client: CopilotSdkClient = yield* Effect.acquireRelease(
        Effect.tryPromise({
          try: () =>
            makeClient({
              cliPath,
              cwd,
              env: Object.fromEntries(
                Object.entries(environment).filter(
                  (entry): entry is [string, string] => entry[1] !== undefined,
                ),
              ),
            }),
          catch: openError,
        }),
        (created) =>
          Effect.tryPromise(() => created.stop()).pipe(
            Effect.timeoutOption("15 seconds"),
            Effect.flatMap((stopped) =>
              Option.isNone(stopped) ? Effect.tryPromise(() => created.forceStop()) : Effect.void,
            ),
            Effect.ignore,
          ),
      );

      const now = yield* DateTime.now;
      let providerSession: OrchestrationV2ProviderSession = {
        id: input.providerSessionId,
        driver: DRIVER,
        providerInstanceId: options.instanceId,
        status: "ready",
        cwd,
        model: input.modelSelection.model,
        capabilities: CopilotSdkCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const events = yield* Queue.unbounded<
        ProviderAdapter.ProviderAdapterV2Event,
        ProviderAdapter.ProviderAdapterV2Error | Cause.Done
      >();
      const inbox = yield* Queue.unbounded<Inbox>();
      const commands = yield* Semaphore.make(1);
      const eventPermit = yield* Semaphore.make(1);
      let session: CopilotSdkSession | undefined;
      let sessionEpoch = 0;
      let thread: OrchestrationV2ProviderThread | undefined;
      let active: ActiveTurn | undefined;
      // Settings Copilot is known to hold; unknown fields are set on the next turn.
      let applied: Partial<CopilotSessionSettings> = {};
      let currentSettings = copilotSessionSettings(input.modelSelection, input.runtimePolicy);
      let closed = false;

      const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
        Queue.offer(events, event).pipe(Effect.asVoid);
      const updateSession = Effect.fnUntraced(function* (
        status: OrchestrationV2ProviderSession["status"],
        lastError: string | null = null,
      ) {
        providerSession = { ...providerSession, status, lastError, updatedAt: yield* DateTime.now };
        yield* emit({ type: "provider_session.updated", driver: DRIVER, providerSession });
      });
      const updateThread = Effect.fnUntraced(function* (
        patch: Partial<OrchestrationV2ProviderThread>,
      ) {
        if (!thread) return;
        thread = { ...thread, ...patch, updatedAt: yield* DateTime.now };
        yield* emit({ type: "provider_thread.updated", driver: DRIVER, providerThread: thread });
      });

      const itemIdentity = (turn: ActiveTurn, nativeItemId: string) =>
        `${options.instanceId}:${turn.input.providerThread.id}:${turn.nativeTurnId}:${nativeItemId}`;
      const baseItem = (
        turn: ActiveTurn,
        nativeItemId: string,
        time: DateTime.Utc,
        status: OrchestrationV2TurnItemStatus,
      ) => {
        if (!turn.ordinals.has(nativeItemId)) {
          turn.ordinals.set(
            nativeItemId,
            turn.input.providerTurnOrdinal * 100 + turn.ordinals.size + 1,
          );
        }
        if (!turn.startedAt.has(nativeItemId)) turn.startedAt.set(nativeItemId, time);
        const identity = itemIdentity(turn, nativeItemId);
        const settled = status !== "running" && status !== "pending" && status !== "waiting";
        return {
          id: idAllocator.derive.turnItemFromProviderItem({
            driver: DRIVER,
            nativeItemId: identity,
          }),
          nodeId: idAllocator.derive.nodeFromProviderItem({
            driver: DRIVER,
            nativeItemId: identity,
          }),
          threadId: turn.input.threadId,
          runId: turn.input.runId,
          providerThreadId: turn.input.providerThread.id,
          providerTurnId: turn.providerTurn.id,
          nativeItemRef: nativeRef(nativeItemId),
          parentItemId: null,
          ordinal: turn.ordinals.get(nativeItemId)!,
          status,
          title: null,
          startedAt: turn.startedAt.get(nativeItemId)!,
          completedAt: settled ? time : null,
          updatedAt: time,
        };
      };
      const publishItem = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        kind: "assistant_message" | "reasoning" | "tool_call",
        turnItem: OrchestrationV2TurnItem & { readonly nodeId: NodeId },
      ) {
        yield* emit({
          type: "node.updated",
          driver: DRIVER,
          node: {
            id: turnItem.nodeId,
            threadId: turnItem.threadId,
            runId: turnItem.runId,
            parentNodeId: turn.input.rootNodeId,
            rootNodeId: turn.input.rootNodeId,
            kind,
            status: turnItem.status,
            countsForRun: false,
            providerThreadId: turn.input.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            nativeItemRef: turnItem.nativeItemRef,
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt: turnItem.startedAt,
            completedAt: turnItem.completedAt,
          },
        });
        yield* emit({ type: "turn_item.updated", driver: DRIVER, turnItem });
      });

      const publishText = Effect.fnUntraced(function* (turn: ActiveTurn, key: string) {
        const segment = turn.texts.get(key);
        if (!segment) return;
        segment.emitted = true;
        turn.dirty.delete(key);
        const time = yield* DateTime.now;
        const base = baseItem(turn, key, time, segment.streaming ? "running" : "completed");
        if (segment.kind === "reasoning") {
          return yield* publishItem(turn, "reasoning", {
            ...base,
            type: "reasoning",
            text: segment.text,
            streaming: segment.streaming,
          });
        }
        const messageId = idAllocator.derive.messageFromProviderItem({
          driver: DRIVER,
          nativeItemId: itemIdentity(turn, key),
        });
        const message: OrchestrationV2ConversationMessage = {
          id: messageId,
          threadId: turn.input.threadId,
          runId: turn.input.runId,
          nodeId: base.nodeId,
          role: "assistant",
          text: segment.text,
          attachments: [],
          streaming: segment.streaming,
          createdBy: "agent",
          creationSource: "provider",
          createdAt: base.startedAt,
          updatedAt: time,
        };
        yield* emit({ type: "message.updated", driver: DRIVER, message });
        yield* publishItem(turn, "assistant_message", {
          ...base,
          type: "assistant_message",
          messageId,
          text: segment.text,
          streaming: segment.streaming,
        });
      });
      // The first chunk shows at once; later ones coalesce so a long reply stays linear.
      const appendText = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        key: string,
        kind: TextSegment["kind"],
        delta: string,
      ) {
        const segment = turn.texts.get(key) ?? { kind, text: "", streaming: true, emitted: false };
        turn.texts.set(key, segment);
        if (!segment.streaming || delta.length === 0) return;
        segment.text += delta;
        if (!segment.emitted) return yield* publishText(turn, key);
        turn.dirty.add(key);
        if (turn.flushScheduled) return;
        turn.flushScheduled = true;
        yield* Effect.sleep(TEXT_FLUSH_INTERVAL).pipe(
          Effect.andThen(Queue.offer(inbox, { type: "flush", turn })),
          Effect.forkIn(scope),
        );
      });
      // The final event carries the whole text: it replaces the deltas instead of adding to them.
      const completeText = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        key: string,
        kind: TextSegment["kind"],
        text: string,
      ) {
        const segment = turn.texts.get(key);
        // A model response that only calls tools has no text worth a row.
        if (!segment && text.trim().length === 0) return;
        turn.texts.set(key, {
          kind,
          text: text.length > 0 ? text : (segment?.text ?? ""),
          streaming: false,
          emitted: segment?.emitted ?? false,
        });
        yield* publishText(turn, key);
      });

      const toolTurnItem = (
        base: ReturnType<typeof baseItem>,
        tool: ToolCall,
        output: unknown,
      ): OrchestrationV2TurnItem & { readonly nodeId: NodeId } => {
        const completion = tool.completion;
        const resultText = completion?.result?.content ?? completion?.error?.message;
        const path = stringField(tool.arguments, "path", "file_path", "filePath");
        const title = tool.title?.trim() || null;
        if (tool.mcpServerName !== undefined) {
          return {
            ...base,
            ...mcpToolPresentation({
              serverName: tool.mcpServerName,
              toolName: tool.mcpToolName ?? tool.toolName,
            }),
            type: "dynamic_tool",
            toolName: `${tool.mcpServerName}.${tool.mcpToolName ?? tool.toolName}`,
            input: tool.arguments ?? {},
            ...(output === undefined ? {} : { output }),
          };
        }
        if (SHELL_TOOLS.has(tool.toolName) || completion?.shellExecution !== undefined) {
          return {
            ...base,
            title,
            type: "command_execution",
            input:
              stringField(tool.arguments, "command") ?? tool.displayCommand ?? title ?? "Command",
            ...(resultText === undefined ? {} : { output: resultText }),
            ...(completion?.shellExecution === undefined
              ? {}
              : { exitCode: completion.shellExecution.exitCode }),
          };
        }
        const edits = (completion?.fileEdits ?? []).filter((edit) => edit.path.trim().length > 0);
        if (EDIT_TOOLS.has(tool.toolName) || edits.length > 0) {
          const detailed = completion?.result?.detailedContent;
          return {
            ...base,
            title,
            type: "file_change",
            fileName: edits[0]?.path.trim() ?? path ?? title ?? "File change",
            ...(edits.length === 0
              ? {}
              : {
                  changes: edits.map((edit) => ({
                    operation: edit.kind.trim() || "edit",
                    path: edit.path.trim(),
                  })),
                }),
            ...(looksLikeDiff(detailed) ? { diffStr: detailed } : {}),
          };
        }
        if (READ_TOOLS.has(tool.toolName)) {
          return {
            ...base,
            title: path ? formatReadToolLabel(path) : (title ?? "Read file"),
            type: "dynamic_tool",
            toolName: "Read",
            input: tool.arguments ?? {},
            ...(resultText === undefined ? {} : { output: resultText }),
          };
        }
        if (SEARCH_TOOLS.has(tool.toolName)) {
          const pattern = stringField(tool.arguments, "pattern", "query", "glob");
          return {
            ...base,
            title: formatSearchToolLabel(record(tool.arguments)) ?? title,
            type: "file_search",
            ...(pattern === undefined ? {} : { pattern }),
          };
        }
        if (FETCH_TOOLS.has(tool.toolName) || tool.toolName === "web_search") {
          const target = stringField(tool.arguments, "url", "query");
          const fetched = FETCH_TOOLS.has(tool.toolName) && target !== undefined;
          return {
            ...base,
            title,
            type: "web_search",
            ...(target === undefined ? {} : { patterns: [target] }),
            ...(fetched
              ? {
                  results: [
                    {
                      url: target,
                      ...(resultText === undefined ? {} : { snippet: resultText.slice(0, 1_000) }),
                    },
                  ],
                }
              : {}),
          };
        }
        return {
          ...base,
          title: title ?? tool.toolName,
          type: "dynamic_tool",
          toolName: tool.toolName,
          input: tool.arguments ?? {},
          ...(resultText === undefined ? {} : { output: resultText }),
        };
      };
      const publishTool = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        tool: ToolCall,
        terminal?: OrchestrationV2TurnItemStatus,
      ) {
        if (HIDDEN_TOOLS.has(tool.toolName)) return;
        const completion = tool.completion;
        const status =
          terminal ??
          (completion === undefined ? "running" : completion.success ? "completed" : "failed");
        // MCP rows read an MCP tool result, as other providers report it.
        let output: unknown =
          completion === undefined
            ? undefined
            : {
                content: completion.result?.contents ?? [
                  {
                    type: "text",
                    text: completion.result?.content ?? completion.error?.message ?? "",
                  },
                ],
                ...(completion.result?.structuredContent === undefined
                  ? {}
                  : { structuredContent: completion.result.structuredContent }),
                ...(completion.success ? {} : { isError: true }),
              };
        if (
          completion !== undefined &&
          tool.mcpServerName !== undefined &&
          resolveT3McpToolId(`${tool.mcpServerName}.${tool.mcpToolName ?? tool.toolName}`) ===
            HTML_RENDER_TOOL_NAME
        ) {
          // Taking the page also releases it when the call failed.
          const page = yield* mcpSessions.takeHtmlRender(
            turn.input.threadId,
            record(tool.arguments) ?? {},
          );
          if (completion.success && page !== undefined) output = { htmlRender: page };
        }
        const time = yield* DateTime.now;
        yield* publishItem(
          turn,
          "tool_call",
          toolTurnItem(baseItem(turn, `tool:${tool.toolCallId}`, time, status), tool, output),
        );
      });

      const finish = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        status: "completed" | "cancelled" | "interrupted" | "failed",
        detail?: string,
        disposition: "reusable" | "broken" = "reusable",
      ) {
        if (active !== turn) return;
        for (const [key, segment] of turn.texts) {
          if (!segment.streaming) continue;
          segment.streaming = false;
          if (segment.emitted || segment.text.length > 0) yield* publishText(turn, key);
        }
        for (const tool of turn.tools.values()) {
          if (tool.completion === undefined) yield* publishTool(turn, tool, status);
        }
        const completedAt = yield* DateTime.now;
        turn.providerTurn = { ...turn.providerTurn, status, completedAt };
        yield* emit({
          type: "provider_turn.updated",
          driver: DRIVER,
          threadId: turn.input.threadId,
          providerTurn: turn.providerTurn,
        });
        yield* updateThread({ status: disposition === "broken" ? "error" : "idle" });
        yield* updateSession(
          disposition === "broken" ? "error" : "ready",
          status === "failed" ? (detail ?? null) : null,
        );
        active = undefined;
        if (status === "failed") {
          const failure = makeProviderFailure({
            class: disposition === "broken" ? "transport_error" : failureClass(turn.lastError),
            message: detail ?? "Copilot could not finish the turn.",
            ...(turn.lastError?.errorCode === undefined ? {} : { code: turn.lastError.errorCode }),
          });
          const base = baseItem(turn, "failure", completedAt, "failed");
          yield* emit({
            type: "turn_item.updated",
            driver: DRIVER,
            turnItem: { ...base, type: "error", failure },
          });
          yield* emit({
            type: "turn.terminal",
            driver: DRIVER,
            providerThreadId: turn.providerTurn.providerThreadId,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.input.runOrdinal,
            status,
            failure,
            failureItemOrdinal: base.ordinal,
            threadDisposition: disposition,
          });
        } else {
          yield* emit({
            type: "turn.terminal",
            driver: DRIVER,
            providerThreadId: turn.providerTurn.providerThreadId,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.input.runOrdinal,
            status,
            failure: null,
            threadDisposition: disposition,
          });
        }
        yield* Deferred.succeed(turn.done, undefined);
      });

      // Projection runs per SDK event inside the session's span; a span per event is noise.
      const handleEvent = Effect.fnUntraced(function* (event: SessionEvent) {
        const turn = active;
        // A child agent's events never drive the root turn.
        if (!turn || event.agentId !== undefined) return;
        switch (event.type) {
          case "assistant.message_delta":
            return yield* appendText(
              turn,
              `message:${event.data.messageId}`,
              "assistant_message",
              event.data.deltaContent,
            );
          case "assistant.message":
            return yield* completeText(
              turn,
              `message:${event.data.messageId}`,
              "assistant_message",
              event.data.content,
            );
          case "assistant.reasoning_delta":
            return yield* appendText(
              turn,
              `reasoning:${event.data.reasoningId}`,
              "reasoning",
              event.data.deltaContent,
            );
          case "assistant.reasoning":
            return yield* completeText(
              turn,
              `reasoning:${event.data.reasoningId}`,
              "reasoning",
              event.data.content,
            );
          case "tool.execution_start": {
            const tool: ToolCall = {
              toolCallId: event.data.toolCallId,
              toolName: event.data.toolName,
              title: event.data.toolTitle,
              arguments: event.data.arguments,
              mcpServerName: event.data.mcpServerName,
              mcpToolName: event.data.mcpToolName,
              displayCommand: event.data.shellToolInfo?.displayCommand,
            };
            turn.tools.set(tool.toolCallId, tool);
            return yield* publishTool(turn, tool);
          }
          case "tool.shell_output": {
            const tool = turn.tools.get(event.data.toolCallId);
            if (!tool || tool.completion !== undefined) return;
            const time = yield* DateTime.now;
            return yield* emit({
              type: "command_output.delta",
              driver: DRIVER,
              threadId: turn.input.threadId,
              itemId: baseItem(turn, `tool:${tool.toolCallId}`, time, "running").id,
              chunk: event.data.text,
            });
          }
          case "tool.execution_complete": {
            const tool: ToolCall = turn.tools.get(event.data.toolCallId) ?? {
              toolCallId: event.data.toolCallId,
              toolName: event.data.toolDescription?.name ?? "tool",
              title: undefined,
              arguments: undefined,
              mcpServerName: undefined,
              mcpToolName: undefined,
              displayCommand: undefined,
            };
            tool.completion = event.data;
            turn.tools.set(tool.toolCallId, tool);
            return yield* publishTool(turn, tool);
          }
          case "session.error":
            turn.lastError = event.data;
            return;
        }
      });
      const settle = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        exit: Exit.Exit<unknown, ProviderAdapter.ProviderAdapterProtocolError>,
      ) {
        if (Exit.isSuccess(exit)) return yield* finish(turn, "completed");
        if (turn.interruptRequested) return yield* finish(turn, "interrupted");
        const failure = Cause.squash(exit.cause);
        const reason = errorText(isProtocolError(failure) ? failure.cause : failure);
        if (/\bdisconnected\b/i.test(reason ?? "")) {
          return yield* finish(turn, "failed", "Copilot stopped unexpectedly.", "broken");
        }
        if (/\baborted\b/i.test(reason ?? "")) return yield* finish(turn, "cancelled");
        yield* finish(turn, "failed", turn.lastError?.message ?? reason);
      });
      const handleInbox = Effect.fnUntraced(
        function* (entry: Inbox) {
          if (entry.type === "event") {
            if (entry.epoch === sessionEpoch) yield* handleEvent(entry.event);
            return;
          }
          if (active !== entry.turn) return;
          if (entry.type === "settled") return yield* settle(entry.turn, entry.exit);
          entry.turn.flushScheduled = false;
          for (const key of entry.turn.dirty) yield* publishText(entry.turn, key);
        },
        Effect.catchCause((cause) =>
          Effect.logWarning("Could not project a Copilot SDK event", cause),
        ),
        eventPermit.withPermits(1),
      );
      yield* Stream.fromQueue(inbox).pipe(Stream.runForEach(handleInbox), Effect.forkIn(scope));
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          closed = true;
          sessionEpoch++;
          if (active) yield* finish(active, "cancelled", "Copilot session closed", "broken");
          yield* Queue.shutdown(inbox);
          yield* Queue.shutdown(events);
        }).pipe(eventPermit.withPermits(1)),
      );

      const onPermissionRequest: PermissionHandler = (request) => {
        const policy = active?.input.runtimePolicy ?? input.runtimePolicy;
        if (request.managedApprovalRequired !== true) {
          if (currentSettings.allowAll || request.kind === "read") return { kind: "approve-once" };
          if (policy.runtimeMode === "auto-accept-edits" && request.kind === "write") {
            return { kind: "approve-once" };
          }
        }
        return {
          kind: "reject",
          feedback:
            "T3 Code cannot show Copilot approval prompts yet, so this action was declined. If it is needed, ask the user to give this thread full access or turn on Allow all.",
        };
      };
      const systemMessage = (
        mcp: McpProviderSession.McpProviderSessionConfig | undefined,
      ): NonNullable<SessionConfig["systemMessage"]> => ({
        mode: "customize",
        sections: { tone: { action: "remove" } },
        // System content survives Copilot's compactions; model and mode stay out because they change.
        content: [
          buildRuntimeInstructions({ harness: "GitHub Copilot" }),
          mcp?.browserToolsAvailable === true ? T3_CODE_BROWSER_TOOL_INSTRUCTIONS.trim() : "",
          t3OrchestrationSystemPrompt(
            mcp !== undefined,
            mcp?.capabilities?.has("device") === true,
          )?.trim() ?? "",
        ]
          .filter((part) => part.length > 0)
          .join("\n\n"),
      });

      const register = Effect.fnUntraced(function* (
        args: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
      ) {
        if (closed) return yield* protocolError("The Copilot session is closed");
        if (active) {
          return yield* protocolError("Cannot change Copilot sessions during an active turn");
        }
        const existing = args.existingProviderThread;
        if (
          existing &&
          (existing.driver !== DRIVER || existing.providerInstanceId !== options.instanceId)
        ) {
          return yield* protocolError("This thread belongs to another provider instance");
        }
        const requestedId = args.forceNewNativeThread
          ? undefined
          : (existing?.nativeThreadRef?.nativeId ?? input.initialNativeThreadId);
        if (
          thread &&
          session &&
          (requestedId === undefined || requestedId === session.sessionId) &&
          thread.appThreadId === args.threadId
        ) {
          return thread;
        }
        if (session) {
          return yield* protocolError("Open a new Copilot session to attach another thread");
        }
        const mcp = yield* mcpSessions.read(args.threadId);
        const settings = copilotSessionSettings(args.modelSelection, args.runtimePolicy);
        const epoch = ++sessionEpoch;
        const config = {
          clientName: "t3-code",
          model: settings.model,
          ...(settings.reasoningEffort === undefined
            ? {}
            : { reasoningEffort: settings.reasoningEffort }),
          workingDirectory: cwd,
          streaming: true,
          systemMessage: systemMessage(mcp),
          ...(mcp === undefined
            ? {}
            : {
                mcpServers: {
                  "t3-code": {
                    type: "http" as const,
                    url: mcp.endpoint,
                    headers: { Authorization: mcp.authorizationHeader },
                    tools: ["*"],
                  },
                },
              }),
          onPermissionRequest,
          onEvent: (event: SessionEvent) => {
            Queue.offerUnsafe(inbox, { type: "event", epoch, event });
          },
        } satisfies SessionConfig;
        // A failed resume must surface: a silent new session would drop the conversation.
        const opened = yield* Effect.tryPromise({
          try: () =>
            requestedId === undefined
              ? client.createSession(config)
              : client.resumeSession(requestedId, config),
          catch: (cause) =>
            protocolError(
              requestedId === undefined
                ? "Copilot could not start a session"
                : "Copilot could not resume this session",
              cause,
            ),
        });
        session = opened;
        currentSettings = settings;
        applied = { model: settings.model, reasoningEffort: settings.reasoningEffort };
        const createdAt = yield* DateTime.now;
        thread = existing
          ? {
              ...existing,
              providerSessionId: input.providerSessionId,
              nativeThreadRef: nativeRef(opened.sessionId),
              status: "idle",
              updatedAt: createdAt,
            }
          : {
              id: idAllocator.derive.providerThread({
                driver: DRIVER,
                providerInstanceId: options.instanceId,
                nativeThreadId: opened.sessionId,
              }),
              driver: DRIVER,
              providerInstanceId: options.instanceId,
              providerSessionId: input.providerSessionId,
              appThreadId: args.threadId,
              ownerNodeId: null,
              nativeThreadRef: nativeRef(opened.sessionId),
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              createdAt,
              updatedAt: createdAt,
            };
        providerSession = { ...providerSession, model: settings.model };
        yield* emit({ type: "provider_thread.updated", driver: DRIVER, providerThread: thread });
        return thread;
      });

      const validateThread = (candidate: OrchestrationV2ProviderThread) =>
        thread !== undefined &&
        session !== undefined &&
        candidate.driver === DRIVER &&
        candidate.providerInstanceId === options.instanceId &&
        candidate.appThreadId === thread.appThreadId &&
        candidate.nativeThreadRef?.nativeId === session.sessionId;
      const call = <A>(operation: string, run: () => Promise<A>) =>
        Effect.tryPromise({
          try: run,
          catch: (cause) => protocolError(`Copilot ${operation} failed`, cause),
        });
      const applySettings = Effect.fnUntraced(function* (
        target: CopilotSdkSession,
        settings: CopilotSessionSettings,
      ) {
        if (
          applied.model !== settings.model ||
          applied.reasoningEffort !== settings.reasoningEffort
        ) {
          yield* call("model switch", () =>
            target.setModel(
              settings.model,
              settings.reasoningEffort === undefined
                ? undefined
                : { reasoningEffort: settings.reasoningEffort },
            ),
          );
        }
        if (applied.mode !== settings.mode) {
          yield* call("mode switch", () => target.rpc.mode.set({ mode: settings.mode }));
        }
        if (applied.allowAll !== settings.allowAll) {
          yield* call("permission mode switch", () =>
            target.rpc.permissions.setMode({ mode: settings.allowAll ? "allow-all" : "manual" }),
          );
        }
        applied = settings;
        currentSettings = settings;
        providerSession = { ...providerSession, model: settings.model };
      });
      const prompt = Effect.fnUntraced(function* (
        message: ProviderAdapter.ProviderAdapterV2TurnMessage,
      ) {
        const text = yield* providerMessageTextWithAttachmentPaths({
          text: message.text,
          attachments: message.attachments,
          resolveAttachmentPath: providerHost.resolveAttachmentPath,
          resolveExtractedTextPath: providerHost.resolveExtractedTextPath,
        });
        const attachments = message.attachments
          .filter(isProviderNativeImageAttachment)
          .flatMap((attachment) => {
            const path = providerHost.resolveAttachmentPath(attachment);
            return path === null ? [] : [{ type: "file" as const, path }];
          });
        if (text.trim().length === 0 && attachments.length === 0) {
          return yield* protocolError("Copilot needs text or an image");
        }
        return {
          prompt: text,
          ...(attachments.length === 0 ? {} : { attachments }),
        } satisfies MessageOptions;
      });

      const start = Effect.fnUntraced(function* (
        turnInput: ProviderAdapter.ProviderAdapterV2TurnInput,
      ) {
        const target = session;
        if (!target || !validateThread(turnInput.providerThread) || closed) {
          return yield* protocolError("This thread is not attached to the Copilot session");
        }
        if (active) {
          return yield* new ProviderAdapter.ProviderAdapterProtocolError({
            driver: DRIVER,
            detail: "Copilot already has an active turn; queue through the orchestrator",
            reason: "previous_turn_active",
          });
        }
        const message = yield* prompt(turnInput.message);
        yield* applySettings(
          target,
          copilotSessionSettings(turnInput.modelSelection, turnInput.runtimePolicy),
        );
        const nativeTurnId = `${target.sessionId}:${turnInput.attemptId}`;
        const startedAt = yield* DateTime.now;
        const turn: ActiveTurn = {
          input: turnInput,
          providerTurn: {
            id: idAllocator.derive.providerTurn({
              driver: DRIVER,
              nativeTurnId: `${options.instanceId}:${nativeTurnId}`,
            }),
            providerThreadId: turnInput.providerThread.id,
            nodeId: turnInput.rootNodeId,
            runAttemptId: turnInput.attemptId,
            nativeTurnRef: nativeRef(nativeTurnId, "weak"),
            ordinal: turnInput.providerTurnOrdinal,
            status: "running",
            startedAt,
            completedAt: null,
          },
          nativeTurnId,
          ordinals: new Map(),
          startedAt: new Map(),
          texts: new Map(),
          tools: new Map(),
          dirty: new Set(),
          done: yield* Deferred.make<void>(),
          flushScheduled: false,
          interruptRequested: false,
          lastError: undefined,
        };
        yield* Effect.gen(function* () {
          thread = turnInput.providerThread;
          active = turn;
          yield* emit({
            type: "provider_turn.updated",
            driver: DRIVER,
            threadId: turnInput.threadId,
            providerTurn: turn.providerTurn,
          });
          yield* updateThread({
            status: "active",
            firstRunOrdinal: thread.firstRunOrdinal ?? turnInput.runOrdinal,
            lastRunOrdinal: turnInput.runOrdinal,
          });
          yield* updateSession("running");
        }).pipe(eventPermit.withPermits(1));
        // sendAndWait correlates completion with this request, waits out autopilot and rejects
        // when the CLI exits. Its outcome queues behind the events that preceded it.
        yield* Effect.tryPromise({
          try: () => target.sendAndWait(message, TURN_COMPLETION_TIMEOUT_MS),
          catch: (cause) => protocolError("Copilot could not finish the turn", cause),
        }).pipe(
          Effect.exit,
          Effect.flatMap((exit) => Queue.offer(inbox, { type: "settled", turn, exit })),
          Effect.forkIn(scope),
        );
      });

      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver: DRIVER,
        providerSessionId: input.providerSessionId,
        providerSession,
        events: Stream.fromQueue(events),
        ensureThread: (args) =>
          register(args).pipe(
            commands.withPermits(1),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterEnsureThreadError({
                  driver: DRIVER,
                  threadId: args.threadId,
                  cause,
                }),
            ),
          ),
        resumeThread: (args) =>
          register({
            threadId: args.threadId ?? args.providerThread.appThreadId ?? input.threadId,
            modelSelection: args.modelSelection ?? input.modelSelection,
            runtimePolicy: args.runtimePolicy ?? input.runtimePolicy,
            existingProviderThread: args.providerThread,
          }).pipe(
            commands.withPermits(1),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterResumeThreadError({
                  driver: DRIVER,
                  providerSessionId: input.providerSessionId,
                  providerThreadId: args.providerThread.id,
                  cause,
                }),
            ),
          ),
        startTurn: (args) =>
          start(args).pipe(
            commands.withPermits(1),
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver: DRIVER,
                  threadId: args.threadId,
                  providerThreadId: args.providerThread.id,
                  runId: args.runId,
                  cause,
                }),
            ),
          ),
        steerTurn: Effect.fn("CopilotSdkAdapter.steerTurn")(
          function* (args) {
            const turn = active;
            const target = session;
            if (
              !turn ||
              !target ||
              turn.providerTurn.id !== args.providerTurnId ||
              !validateThread(args.providerThread)
            ) {
              return yield* protocolError("This Copilot turn cannot be steered");
            }
            const message = yield* prompt(args.message);
            // Immediate delivery joins the running turn and leaves background agents running.
            yield* call("steering", () => target.send({ ...message, mode: "immediate" }));
          },
          (effect, args) =>
            effect.pipe(
              commands.withPermits(1),
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterSteerRunError({
                    driver: DRIVER,
                    providerThreadId: args.providerThread.id,
                    providerTurnId: args.providerTurnId,
                    cause,
                  }),
              ),
            ),
        ),
        interruptTurn: Effect.fn("CopilotSdkAdapter.interruptTurn")(
          function* (args) {
            const turn = active;
            const target = session;
            if (
              !turn ||
              !target ||
              turn.providerTurn.id !== args.providerTurnId ||
              !validateThread(args.providerThread)
            ) {
              return yield* protocolError("This Copilot turn is no longer active");
            }
            turn.interruptRequested = true;
            yield* call("abort", () => target.abort());
            // abort() only acknowledges the request; the turn ends when its send settles.
            const confirmed = yield* Deferred.await(turn.done).pipe(
              Effect.timeoutOption(INTERRUPT_CONFIRMATION_TIMEOUT),
            );
            if (Option.isNone(confirmed)) {
              yield* finish(turn, "interrupted", undefined, "broken").pipe(
                eventPermit.withPermits(1),
              );
            }
          },
          (effect, args) =>
            effect.pipe(
              commands.withPermits(1),
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterInterruptError({
                    driver: DRIVER,
                    providerThreadId: args.providerThread.id,
                    providerTurnId: args.providerTurnId,
                    cause,
                  }),
              ),
            ),
        ),
        respondToRuntimeRequest: (args) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
              driver: DRIVER,
              requestId: args.requestId,
              cause: "This Copilot session has no pending requests.",
            }),
          ),
        readThreadSnapshot: (args) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterReadThreadSnapshotError({
              driver: DRIVER,
              providerThreadId: args.providerThread.id,
              cause: "Copilot thread snapshots are not supported in T3 Code.",
            }),
          ),
        rollbackThread: (args) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterRollbackThreadError({
              driver: DRIVER,
              providerThreadId: args.providerThread.id,
              checkpointId: args.target.checkpointId,
              cause: "Copilot conversation rollback is not supported in T3 Code.",
            }),
          ),
        forkThread: (args) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterForkThreadError({
              driver: DRIVER,
              providerThreadId: args.sourceProviderThread.id,
              cause: "Copilot native forks are not supported in T3 Code.",
            }),
          ),
      };
      return runtime;
    }),
  });
});
