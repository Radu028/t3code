import type {
  MessageOptions,
  PermissionRequest,
  SessionConfig,
  SessionEvent,
  SessionEventPayload,
} from "@github/copilot-sdk";
import {
  ProviderDriverKind,
  type ModelSelection,
  type NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2Subagent,
  type OrchestrationV2TurnItem,
  type OrchestrationV2TurnItemStatus,
  type PlanId,
  type ProviderApprovalOption,
  type ProviderInstanceId,
  type ProviderRequestKind,
  type ProviderThreadId,
  type ProviderTurnId,
  type RunId,
  type RuntimeRequestId,
  type ThreadId,
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
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

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
import {
  makeSubagentChildThread,
  makeSubagentConversationArtifacts,
  subagentThreadTitle,
} from "@t3tools/provider-core/server/subagentProjection";
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
// T3 runs a thread's effects one at a time, so a Stop queued behind an RPC waits for it. The
// turn's own request and /compact are Copilot's work, not an answer, and stay unbounded.
const RPC_TIMEOUT: Duration.Input = "30 seconds";
// Starting the CLI, or opening or resuming a session, can start MCP servers and load a long
// transcript.
const SESSION_OPEN_TIMEOUT: Duration.Input = "2 minutes";
const TEXT_FLUSH_INTERVAL: Duration.Input = "50 millis";
const CLIENT_STOP_TIMEOUT: Duration.Input = "15 seconds";
// The CLI ends waitForPending itself after ten minutes; this bounds an answer that never comes.
const BACKGROUND_WAIT_TIMEOUT: Duration.Input = "11 minutes";

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
    supportsCommandApproval: true,
    supportsFileReadApproval: true,
    supportsFileChangeApproval: true,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: true,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: true,
  },
  planning: {
    emitsPlanUpdated: true,
    emitsTodoList: false,
    emitsProposedPlan: true,
    supportsStructuredQuestions: true,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: true,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: true,
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
    nativeRequestIds: "strong",
  },
} satisfies OrchestrationV2ProviderCapabilities;

type CopilotMode = Parameters<CopilotSdkSession["rpc"]["mode"]["set"]>[0]["mode"];
type CopilotReasoningEffort = NonNullable<SessionConfig["reasoningEffort"]>;
type PermissionDecision = Parameters<
  CopilotSdkSession["rpc"]["permissions"]["handlePendingPermissionRequest"]
>[0]["result"];
type UserInputHandler = NonNullable<SessionConfig["onUserInputRequest"]>;
type UserInputRequest = Parameters<UserInputHandler>[0];
type UserInputResponse = Awaited<ReturnType<UserInputHandler>>;
type ToolCompletion = SessionEventPayload<"tool.execution_complete">["data"];
type CopilotError = SessionEventPayload<"session.error">["data"];
type CompactResult = Awaited<ReturnType<CopilotSdkSession["rpc"]["history"]["compact"]>>;

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

/**
 * Where items land: the root turn, or a subagent's child thread. Child items have no
 * run and hang off the child thread's root node, as the shared subagent projection expects.
 */
interface ItemScope {
  readonly threadId: ThreadId;
  readonly runId: RunId | null;
  readonly providerThreadId: ProviderThreadId | null;
  readonly providerTurnId: ProviderTurnId | null;
  readonly rootNodeId: NodeId;
  readonly sharesRootNode: boolean;
  /** `<instance>:<session>`; item keys are Copilot's own ids, so ids stay short and stable. */
  readonly identity: string;
  readonly ordinalBase: number;
  readonly appThread: OrchestrationV2AppThread;
  readonly modelSelection: ModelSelection;
  /** The root thread, whose MCP session child agents share. */
  readonly mcpThreadId: ThreadId;
  readonly ordinals: Map<string, number>;
  readonly startedAt: Map<string, DateTime.Utc>;
  readonly texts: Map<string, TextSegment>;
  readonly tools: Map<string, ToolCall>;
  readonly dirty: Set<string>;
  flushScheduled: boolean;
}

type SubagentStatus = "running" | "completed" | "failed" | "cancelled" | "interrupted";

interface Subagent {
  readonly toolCallId: string;
  /** `<session>:task:<tool call>`, the prefix of the subagent's own node and child-thread ids. */
  readonly identity: string;
  readonly parent: ItemScope;
  readonly scope: ItemScope;
  readonly background: boolean;
  readonly prompt: string;
  title: string;
  model: string | null;
  status: SubagentStatus;
  result: string | null;
  completedAt: DateTime.Utc | null;
}

interface ActiveTurn {
  readonly input: ProviderAdapter.ProviderAdapterV2TurnInput;
  providerTurn: OrchestrationV2ProviderTurn;
  readonly nativeTurnId: string;
  readonly mode: CopilotMode;
  readonly scope: ItemScope;
  readonly subagents: Map<string, Subagent>;
  readonly agents: Map<string, Subagent>;
  readonly usageIds: Set<string>;
  readonly done: Deferred.Deferred<void>;
  planId: PlanId | undefined;
  /** The running compaction's item, settled with the turn if Copilot never completes it. */
  compaction: (OrchestrationV2TurnItem & { readonly nodeId: NodeId }) | undefined;
  /** Steering messages Copilot has not yet taken into the conversation. */
  readonly steers: Set<string>;
  /** A steer arrived after the request's run ended and is running as its own run. */
  followUpRunning: boolean;
  /** sendAndWait succeeded; the turn ends once no steer or follow-up run is outstanding. */
  requestSettled: boolean;
  /** A follow-up run's error that arrived before the request settled. */
  followUpFailure: CopilotError | undefined;
  /** Autopilot settled; the turn waits for Copilot's background work to drain. */
  draining: boolean;
  /** A drain returned with work still running; the next background-task change re-checks. */
  drainWaiting: boolean;
  /** Background tasks changed while a drain was in flight, so its task list may be stale. */
  tasksChanged: boolean;
  /** Drains in a row whose task list Copilot could not return. */
  listFailures: number;
  /** An autopilot turn that ended without completing, until T3 knows its background work stopped. */
  ending:
    | { readonly status: "failed" | "cancelled" | "interrupted"; readonly detail?: string }
    | undefined;
  /** T3 is killing the CLI; the kill path, not the rejected request, ends the turn. */
  forcedStop: boolean;
  interruptRequested: boolean;
  lastError: CopilotError | undefined;
}

interface PendingRequest {
  readonly turn: ActiveTurn;
  readonly native:
    | {
        readonly type: "permission";
        readonly requestId: string;
        readonly forSession: PermissionDecision | undefined;
      }
    | {
        readonly type: "question";
        readonly request: UserInputRequest;
        readonly resolve: (response: UserInputResponse) => void;
        readonly reject: (cause: Error) => void;
      };
  request: OrchestrationV2RuntimeRequest;
  node: OrchestrationV2ExecutionNode;
  item: OrchestrationV2TurnItem;
  /** T3 answered and persisted the resolution; the adapter must not republish it. */
  answered: boolean;
}

type Inbox =
  | { readonly type: "event"; readonly epoch: number; readonly event: SessionEvent }
  | { readonly type: "flush"; readonly scope: ItemScope }
  | {
      readonly type: "settled";
      readonly turn: ActiveTurn;
      readonly exit: Exit.Exit<unknown, ProviderAdapter.ProviderAdapterProtocolError>;
    }
  | { readonly type: "plan"; readonly markdown: string }
  | {
      readonly type: "compacted";
      readonly turn: ActiveTurn;
      readonly exit: Exit.Exit<CompactResult, ProviderAdapter.ProviderAdapterProtocolError>;
    }
  | {
      /** The task list for a turn that ended without completing. */
      readonly type: "ending";
      readonly turn: ActiveTurn;
      readonly busy: boolean | undefined;
    }
  | {
      readonly type: "drained";
      readonly turn: ActiveTurn;
      /** Whether Copilot still lists a running task; undefined when it could not say. */
      readonly busy: boolean | undefined;
    }
  | { readonly type: "exited"; readonly client: CopilotSdkClient }
  | {
      readonly type: "question";
      readonly request: UserInputRequest;
      readonly resolve: (response: UserInputResponse) => void;
      readonly reject: (cause: Error) => void;
    };

// Copilot CLI's built-in tools, by the row T3 renders for them.
const SHELL_TOOLS = new Set(["bash", "powershell"]);
const EDIT_TOOLS = new Set(["edit", "create", "apply_patch", "str_replace_editor", "write"]);
const READ_TOOLS = new Set(["view", "read", "read_file"]);
const SEARCH_TOOLS = new Set(["grep", "glob", "rg", "search"]);
const FETCH_TOOLS = new Set(["web_fetch", "fetch"]);
const SUBAGENT_TOOL = "task";
// report_intent drives Copilot's status line; exit_plan_mode shows as the proposed plan.
const HIDDEN_TOOLS = new Set(["report_intent", "exit_plan_mode"]);
const QUESTION_ID = "answer";
const QUESTION_DECLINED = "The user declined to answer.";
// With T3's MCP server the main agent delegates through delegate_task, so T3 owns child threads,
// waits and restarts. Copilot matches these as runtime tool names; builtin: patterns do not apply
// here, and MCP tools are always <server>-<tool>, so they cannot collide.
const COPILOT_DELEGATION_TOOLS = ["task", "read_agent", "write_agent", "list_agents"];
// Models with a long-context tier use it; a model switch drops the tier unless it is passed again.
const CONTEXT_TIER = "long_context";
// Drains that may fail to list Copilot's tasks before an autopilot turn ends without knowing.
const DRAIN_LIST_ATTEMPTS = 3;
const PLAN_CAPTURED_FEEDBACK =
  "T3 Code captured your proposed plan. Stop here and wait for the user's feedback or implementation request in a later turn.";

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

/** Keys for items without a Copilot id; T3's turn ordinal keeps them distinct across turns. */
const turnKey = (turn: { readonly input: { readonly providerTurnOrdinal: number } }) =>
  `turn:${turn.input.providerTurnOrdinal}`;

const settledStatus = (status: OrchestrationV2TurnItemStatus) =>
  status !== "running" && status !== "pending" && status !== "waiting";

const failureClass = (error: CopilotError | undefined) =>
  error?.statusCode === 429 || /rate.?limit|quota/i.test(error?.errorType ?? "")
    ? ("usage_limit" as const)
    : ("provider_error" as const);

const isProtocolError = Schema.is(ProviderAdapter.ProviderAdapterProtocolError);

/**
 * Raised when Copilot refuses a model switch because the conversation does not fit the target
 * model's context. `makeProviderFailure` shows its message to the user.
 */
/**
 * Any failure before Copilot was handed the turn's prompt, such as a settings change it rejected.
 * T3 then keeps the native session for the retry; the cause keeps the message the user sees.
 */
export class CopilotPromptNotSentError extends Schema.TaggedError<CopilotPromptNotSentError>()(
  "CopilotPromptNotSentError",
  { cause: Schema.Defect() },
) {}

class CopilotModelSwitchError extends Schema.TaggedError<CopilotModelSwitchError>()(
  "CopilotModelSwitchError",
  {
    model: Schema.String,
    currentTokens: Schema.Number,
    targetLimit: Schema.Number,
  },
) {
  override get message(): string {
    return `Copilot did not switch to ${this.model}: the conversation (${this.currentTokens} tokens) does not fit its ${this.targetLimit}-token limit. Send /compact, then try again.`;
  }
}

const errorText = (cause: unknown) =>
  cause instanceof Error && cause.message.trim().length > 0 ? cause.message : undefined;

const urlHost = (url: string) => {
  try {
    return new URL(url).hostname || undefined;
  } catch {
    return undefined;
  }
};

/** The T3 approval card for a Copilot permission request, and its "for this session" grant. */
function permissionPresentation(request: PermissionRequest): {
  readonly kind: ProviderRequestKind;
  readonly prompt: string;
  readonly forSession: PermissionDecision | undefined;
} {
  switch (request.kind) {
    case "shell":
      return {
        kind: "command",
        prompt: request.fullCommandText,
        forSession: request.canOfferSessionApproval
          ? {
              kind: "approve-for-session",
              approval: {
                kind: "commands",
                commandIdentifiers: request.commands.map((command) => command.identifier),
              },
            }
          : undefined,
      };
    case "write":
      return {
        kind: "file-change",
        prompt: request.fileName,
        forSession: request.canOfferSessionApproval
          ? { kind: "approve-for-session", approval: { kind: "write" } }
          : undefined,
      };
    case "read":
      return {
        kind: "file-read",
        prompt: request.path,
        forSession: { kind: "approve-for-session", approval: { kind: "read" } },
      };
    case "mcp":
      return {
        kind: "permission",
        prompt: `${request.serverName}.${request.toolName}`,
        forSession: {
          kind: "approve-for-session",
          approval: { kind: "mcp", serverName: request.serverName, toolName: request.toolName },
        },
      };
    case "url": {
      const host = urlHost(request.url);
      return {
        kind: "permission",
        prompt: request.url,
        forSession: host ? { kind: "approve-for-session", domain: host } : undefined,
      };
    }
    default:
      return {
        kind: "permission",
        prompt:
          "toolName" in request && typeof request.toolName === "string"
            ? request.toolName
            : `Copilot asks for ${request.kind} permission`,
        forSession: undefined,
      };
  }
}

export const makeCopilotSdkAdapterV2 = Effect.fn("makeCopilotSdkAdapterV2")(function* (
  options: CopilotSdkAdapterV2Options,
) {
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const providerHost = yield* ProviderHost.ProviderHost;
  const fileSystem = yield* FileSystem.FileSystem;
  const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
  const platform = yield* HostProcess.Platform;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
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
      const sessionScope = yield* Effect.scope;
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
      const launchClient = Effect.gen(function* () {
        const created = yield* Effect.tryPromise({
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
          catch: (cause) => protocolError("Could not start Copilot", cause),
        });
        // Owned before the handshake, so a Stop, a failure or a stall during it still stops the
        // CLI. The SDK gives the handshake no deadline of its own.
        yield* Effect.tryPromise({
          try: () => created.start(),
          catch: (cause) => protocolError("Could not start Copilot", cause),
        }).pipe(
          Effect.timeoutOrElse({
            duration: SESSION_OPEN_TIMEOUT,
            orElse: () => Effect.fail(protocolError("Copilot did not finish starting in time")),
          }),
          Effect.onError(() => terminate(created).pipe(Effect.asVoid)),
        );
        return created;
      });
      // The SDK's kill reaches only copilot.exe on Windows; taskkill /T also stops its shells and agents.
      const killProcessTree = Effect.fnUntraced(function* (pid: number) {
        const exitCode = yield* Effect.scoped(
          Effect.gen(function* () {
            const taskkill = yield* spawner.spawn(
              ChildProcess.make("taskkill", ["/PID", String(pid), "/T", "/F"]),
            );
            return Number(yield* taskkill.exitCode);
          }),
        ).pipe(
          Effect.timeoutOption(CLIENT_STOP_TIMEOUT),
          Effect.orElseSucceed(() => Option.none<number>()),
        );
        if (Option.isNone(exitCode)) return "taskkill did not finish";
        return exitCode.value === 0 ? undefined : `taskkill exited ${exitCode.value}`;
      });
      /** Kills the client's process tree; returns why it may not all have stopped. */
      const terminate = Effect.fnUntraced(function* (target: CopilotSdkClient) {
        // Once the process exited its pid can be recycled, so only a live process is ours to kill.
        const treeFailure =
          platform === "win32" && target.process !== undefined && !target.process.hasExited()
            ? yield* killProcessTree(target.process.id)
            : undefined;
        // forceStop rejects when the CLI outlives its SIGKILL.
        const stopped = yield* Effect.tryPromise(() => target.forceStop()).pipe(
          Effect.timeoutOption(CLIENT_STOP_TIMEOUT),
          Effect.map(Option.isSome),
          Effect.orElseSucceed(() => false),
        );
        // After a failed stop() the SDK forgets its child, so forceStop() no longer kills it.
        if (target.process !== undefined && !target.process.hasExited()) target.process.kill();
        const failure = stopped
          ? treeFailure
          : treeFailure === undefined
            ? "Copilot did not exit"
            : `${treeFailure}, Copilot did not exit`;
        if (failure !== undefined) {
          yield* Effect.logWarning("Copilot's process tree may still be running", { failure });
        }
        return failure;
      });
      let client = yield* launchClient.pipe(Effect.mapError(openError));

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
      // Copilot can exit while no request is pending, so no SDK promise would report it.
      const observeExit = (target: CopilotSdkClient) =>
        target.process === undefined
          ? Effect.void
          : Effect.promise(() => target.process?.exited ?? Promise.resolve()).pipe(
              Effect.andThen(Queue.offer(inbox, { type: "exited", client: target })),
              Effect.forkIn(sessionScope),
              Effect.asVoid,
            );
      yield* observeExit(client);
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          const target = client;
          // stop() reports a CLI it could not kill among its errors rather than rejecting.
          const stopped = yield* Effect.tryPromise(() => target.stop()).pipe(
            Effect.timeoutOption(CLIENT_STOP_TIMEOUT),
            Effect.map((errors) => Option.isSome(errors) && errors.value.length === 0),
            Effect.orElseSucceed(() => false),
          );
          if (stopped) return;
          const failure = yield* terminate(target);
          const cli = target.process;
          if (cli !== undefined && !cli.hasExited()) {
            yield* Effect.promise(() => cli.exited).pipe(Effect.timeoutOption(CLIENT_STOP_TIMEOUT));
          }
          if (failure !== undefined || cli?.hasExited() === false) {
            yield* Effect.logWarning("Copilot may still be running after its session closed", {
              pid: cli?.id,
              failure,
            });
          }
        }),
      );
      const commands = yield* Semaphore.make(1);
      const eventPermit = yield* Semaphore.make(1);
      let session: CopilotSdkSession | undefined;
      let sessionEpoch = 0;
      let thread: OrchestrationV2ProviderThread | undefined;
      let active: ActiveTurn | undefined;
      // Settings Copilot is known to hold; unknown fields are set on the next turn.
      let applied: { [K in keyof CopilotSessionSettings]?: CopilotSessionSettings[K] | undefined } =
        {};
      let currentSettings = copilotSessionSettings(input.modelSelection, input.runtimePolicy);
      let closed = false;
      // The CLI exited or was killed; the next thread registration launches a new one.
      let broken = false;
      const markBroken = () => {
        broken = true;
        session = undefined;
      };
      let questionCount = 0;
      const pending = new Map<RuntimeRequestId, PendingRequest>();
      const pendingPermissions = new Map<string, RuntimeRequestId>();

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
      const emitTurn = (turn: ActiveTurn) =>
        emit({
          type: "provider_turn.updated",
          driver: DRIVER,
          threadId: turn.input.threadId,
          providerTurn: turn.providerTurn,
        });

      const makeScope = (fields: {
        readonly threadId: ThreadId;
        readonly runId: RunId | null;
        readonly providerThreadId: ProviderThreadId | null;
        readonly providerTurnId: ProviderTurnId | null;
        readonly rootNodeId: NodeId;
        readonly sharesRootNode: boolean;
        readonly identity: string;
        readonly ordinalBase: number;
        readonly appThread: OrchestrationV2AppThread;
        readonly modelSelection: ModelSelection;
        readonly mcpThreadId: ThreadId;
      }): ItemScope => ({
        ...fields,
        ordinals: new Map(),
        startedAt: new Map(),
        texts: new Map(),
        tools: new Map(),
        dirty: new Set(),
        flushScheduled: false,
      });
      const baseItem = (
        scope: ItemScope,
        nativeItemId: string,
        time: DateTime.Utc,
        status: OrchestrationV2TurnItemStatus,
      ) => {
        if (!scope.ordinals.has(nativeItemId)) {
          scope.ordinals.set(nativeItemId, scope.ordinalBase + scope.ordinals.size + 1);
        }
        if (!scope.startedAt.has(nativeItemId)) scope.startedAt.set(nativeItemId, time);
        const identity = `${scope.identity}:${nativeItemId}`;
        return {
          id: idAllocator.derive.turnItemFromProviderItem({
            driver: DRIVER,
            nativeItemId: identity,
          }),
          nodeId: scope.sharesRootNode
            ? scope.rootNodeId
            : idAllocator.derive.nodeFromProviderItem({ driver: DRIVER, nativeItemId: identity }),
          threadId: scope.threadId,
          runId: scope.runId,
          providerThreadId: scope.providerThreadId,
          providerTurnId: scope.providerTurnId,
          nativeItemRef: nativeRef(nativeItemId),
          parentItemId: null,
          ordinal: scope.ordinals.get(nativeItemId)!,
          status,
          title: null,
          startedAt: scope.startedAt.get(nativeItemId)!,
          completedAt: settledStatus(status) ? time : null,
          updatedAt: time,
        };
      };
      const publishItem = Effect.fnUntraced(function* (
        scope: ItemScope,
        kind: OrchestrationV2ExecutionNode["kind"],
        turnItem: OrchestrationV2TurnItem & { readonly nodeId: NodeId },
      ) {
        if (!scope.sharesRootNode) {
          yield* emit({
            type: "node.updated",
            driver: DRIVER,
            node: {
              id: turnItem.nodeId,
              threadId: scope.threadId,
              runId: scope.runId,
              parentNodeId: scope.rootNodeId,
              rootNodeId: scope.rootNodeId,
              kind,
              status: turnItem.status,
              countsForRun: false,
              providerThreadId: scope.providerThreadId,
              providerTurnId: scope.providerTurnId,
              nativeItemRef: turnItem.nativeItemRef,
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt: turnItem.startedAt,
              completedAt: turnItem.completedAt,
            },
          });
        }
        yield* emit({ type: "turn_item.updated", driver: DRIVER, turnItem });
      });

      const publishText = Effect.fnUntraced(function* (scope: ItemScope, key: string) {
        const segment = scope.texts.get(key);
        if (!segment) return;
        segment.emitted = true;
        scope.dirty.delete(key);
        const time = yield* DateTime.now;
        const base = baseItem(scope, key, time, segment.streaming ? "running" : "completed");
        if (segment.kind === "reasoning") {
          return yield* publishItem(scope, "reasoning", {
            ...base,
            type: "reasoning",
            text: segment.text,
            streaming: segment.streaming,
          });
        }
        const messageId = idAllocator.derive.messageFromProviderItem({
          driver: DRIVER,
          nativeItemId: `${scope.identity}:${key}`,
        });
        const message: OrchestrationV2ConversationMessage = {
          id: messageId,
          threadId: scope.threadId,
          runId: scope.runId,
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
        yield* publishItem(scope, "assistant_message", {
          ...base,
          type: "assistant_message",
          messageId,
          text: segment.text,
          streaming: segment.streaming,
        });
      });
      // The first chunk shows at once; later ones coalesce so a long reply stays linear.
      const appendText = Effect.fnUntraced(function* (
        scope: ItemScope,
        key: string,
        kind: TextSegment["kind"],
        delta: string,
      ) {
        const segment = scope.texts.get(key) ?? { kind, text: "", streaming: true, emitted: false };
        scope.texts.set(key, segment);
        if (!segment.streaming || delta.length === 0) return;
        segment.text += delta;
        if (!segment.emitted) return yield* publishText(scope, key);
        scope.dirty.add(key);
        if (scope.flushScheduled) return;
        scope.flushScheduled = true;
        yield* Effect.sleep(TEXT_FLUSH_INTERVAL).pipe(
          Effect.andThen(Queue.offer(inbox, { type: "flush", scope })),
          Effect.forkIn(sessionScope),
        );
      });
      // The final event carries the whole text: it replaces the deltas instead of adding to them.
      const completeText = Effect.fnUntraced(function* (
        scope: ItemScope,
        key: string,
        kind: TextSegment["kind"],
        text: string,
      ) {
        const segment = scope.texts.get(key);
        // A model response that only calls tools has no text worth a row.
        if (!segment && text.trim().length === 0) return;
        scope.texts.set(key, {
          kind,
          text: text.length > 0 ? text : (segment?.text ?? ""),
          streaming: false,
          emitted: segment?.emitted ?? false,
        });
        yield* publishText(scope, key);
      });

      const toolTurnItem = (
        base: ReturnType<typeof baseItem>,
        tool: ToolCall,
        output: unknown,
      ): OrchestrationV2TurnItem & { readonly nodeId: NodeId } => {
        const completion = tool.completion;
        // detailedContent is the full UI result; content is the model's, possibly truncated.
        const resultText =
          completion?.result?.detailedContent ??
          completion?.result?.content ??
          completion?.error?.message;
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
        scope: ItemScope,
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
                    text:
                      completion.result?.detailedContent ??
                      completion.result?.content ??
                      completion.error?.message ??
                      "",
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
            scope.mcpThreadId,
            record(tool.arguments) ?? {},
          );
          if (completion.success && page !== undefined) output = { htmlRender: page };
        }
        const time = yield* DateTime.now;
        yield* publishItem(
          scope,
          "tool_call",
          toolTurnItem(baseItem(scope, `tool:${tool.toolCallId}`, time, status), tool, output),
        );
      });
      const settleScope = Effect.fnUntraced(function* (
        scope: ItemScope,
        status: OrchestrationV2TurnItemStatus,
        subagents: ReadonlyMap<string, Subagent>,
      ) {
        for (const [key, segment] of scope.texts) {
          if (!segment.streaming) continue;
          segment.streaming = false;
          if (segment.emitted || segment.text.length > 0) yield* publishText(scope, key);
        }
        for (const tool of scope.tools.values()) {
          if (tool.completion === undefined && !subagents.has(tool.toolCallId)) {
            yield* publishTool(scope, tool, status);
          }
        }
      });

      const publishSubagent = Effect.fnUntraced(function* (subagent: Subagent) {
        const parent = subagent.parent;
        const time = yield* DateTime.now;
        const base = baseItem(parent, `tool:${subagent.toolCallId}`, time, subagent.status);
        // A subagent always has its own node, even inside a child thread.
        const nodeId = idAllocator.derive.nodeFromProviderItem({
          driver: DRIVER,
          nativeItemId: `${subagent.identity}:subagent`,
        });
        const nativeTaskRef = nativeRef(subagent.toolCallId);
        const subagentRecord: OrchestrationV2Subagent = {
          id: nodeId,
          threadId: parent.threadId,
          runId: parent.runId,
          parentNodeId: parent.rootNodeId,
          origin: "provider_native",
          createdBy: "agent",
          driver: DRIVER,
          providerInstanceId: options.instanceId,
          providerThreadId: parent.providerThreadId,
          childThreadId: subagent.scope.threadId,
          nativeTaskRef,
          prompt: subagent.prompt,
          title: subagent.title,
          model: subagent.model,
          status: subagent.status,
          result: subagent.result,
          startedAt: base.startedAt,
          completedAt: subagent.completedAt,
          updatedAt: time,
        };
        const node = {
          countsForRun: false,
          nativeItemRef: nativeTaskRef,
          runtimeRequestId: null,
          checkpointScopeId: null,
          status: subagent.status,
          startedAt: base.startedAt,
          completedAt: subagent.completedAt,
        };
        yield* emit({ type: "subagent.updated", driver: DRIVER, subagent: subagentRecord });
        yield* emit({
          type: "node.updated",
          driver: DRIVER,
          node: {
            ...node,
            id: nodeId,
            threadId: parent.threadId,
            runId: parent.runId,
            parentNodeId: parent.rootNodeId,
            rootNodeId: parent.rootNodeId,
            kind: "subagent",
            providerThreadId: parent.providerThreadId,
            providerTurnId: parent.providerTurnId,
          },
        });
        yield* emit({
          type: "node.updated",
          driver: DRIVER,
          node: {
            ...node,
            id: subagent.scope.rootNodeId,
            threadId: subagent.scope.threadId,
            runId: null,
            parentNodeId: null,
            rootNodeId: subagent.scope.rootNodeId,
            kind: "root_turn",
            providerThreadId: null,
            providerTurnId: null,
          },
        });
        yield* emit({
          type: "turn_item.updated",
          driver: DRIVER,
          turnItem: {
            ...base,
            nodeId,
            nativeItemRef: nativeTaskRef,
            completedAt: subagent.completedAt,
            type: "subagent",
            title: subagent.title,
            subagentId: nodeId,
            origin: "provider_native",
            driver: DRIVER,
            providerInstanceId: options.instanceId,
            childThreadId: subagent.scope.threadId,
            prompt: subagent.prompt,
            result: subagent.result,
          },
        });
      });
      const startSubagent = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        parent: ItemScope,
        tool: ToolCall,
      ) {
        const sessionId = session?.sessionId ?? "";
        const prompt = stringField(tool.arguments, "prompt") ?? "";
        const model = stringField(tool.arguments, "model") ?? null;
        const time = yield* DateTime.now;
        const identity = `${options.instanceId}:${sessionId}:task:${tool.toolCallId}`;
        // The derived thread id already carries the instance.
        const childThreadId = idAllocator.derive.threadFromProviderThread({
          driver: DRIVER,
          providerInstanceId: options.instanceId,
          nativeThreadId: `${sessionId}:task:${tool.toolCallId}`,
        });
        const rootNodeId = idAllocator.derive.nodeFromProviderItem({
          driver: DRIVER,
          nativeItemId: `${identity}:child-root`,
        });
        const title = subagentThreadTitle({
          parentTitle: parent.appThread.title,
          title: stringField(tool.arguments, "description") ?? null,
          prompt,
          ordinal: turn.subagents.size + 1,
        });
        const modelSelection =
          model === null ? parent.modelSelection : { ...parent.modelSelection, model };
        const appThread = makeSubagentChildThread({
          parentThread: parent.appThread,
          childThreadId,
          parentNodeId: idAllocator.derive.nodeFromProviderItem({
            driver: DRIVER,
            nativeItemId: `${identity}:subagent`,
          }),
          activeProviderThreadId: null,
          providerInstanceId: options.instanceId,
          modelSelection,
          title,
          now: time,
          createdBy: "agent",
          creationSource: "provider",
        });
        const subagent: Subagent = {
          toolCallId: tool.toolCallId,
          identity,
          parent,
          // The prompt takes ordinal 100; the child's own items follow it.
          scope: makeScope({
            threadId: childThreadId,
            runId: null,
            providerThreadId: null,
            providerTurnId: null,
            rootNodeId,
            sharesRootNode: true,
            // Copilot's message and tool ids are unique within the session, child items included.
            identity: `${options.instanceId}:${sessionId}`,
            ordinalBase: 100,
            appThread,
            modelSelection,
            mcpThreadId: parent.mcpThreadId,
          }),
          background: stringField(tool.arguments, "mode") === "background",
          prompt,
          title,
          model,
          status: "running",
          result: null,
          completedAt: null,
        };
        turn.subagents.set(tool.toolCallId, subagent);
        yield* emit({ type: "app_thread.created", driver: DRIVER, appThread });
        const promptItem = `${identity}:prompt`;
        const artifacts = makeSubagentConversationArtifacts({
          messageId: idAllocator.derive.messageFromProviderItem({
            driver: DRIVER,
            nativeItemId: promptItem,
          }),
          senderThreadId: parent.threadId,
          turnItemId: idAllocator.derive.turnItemFromProviderItem({
            driver: DRIVER,
            nativeItemId: promptItem,
          }),
          threadId: childThreadId,
          rootNodeId,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: nativeRef(`${tool.toolCallId}:prompt`, "weak"),
          role: "user",
          text: prompt,
          ordinal: 100,
          now: time,
        });
        yield* emit({ type: "message.updated", driver: DRIVER, message: artifacts.message });
        yield* emit({ type: "turn_item.updated", driver: DRIVER, turnItem: artifacts.turnItem });
        yield* publishSubagent(subagent);
      });
      const settleSubagent = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        subagent: Subagent,
        status: Exclude<SubagentStatus, "running">,
        result?: string,
      ) {
        if (subagent.status !== "running") return;
        yield* settleScope(subagent.scope, status, turn.subagents);
        const lastAnswer = [...subagent.scope.texts.values()].findLast(
          (segment) => segment.kind === "assistant_message",
        )?.text;
        subagent.status = status;
        subagent.result = result?.trim() || lastAnswer || null;
        subagent.completedAt = yield* DateTime.now;
        yield* publishSubagent(subagent);
      });

      const publishPlan = Effect.fnUntraced(function* (turn: ActiveTurn, markdown: string) {
        const planId =
          turn.planId ??
          (yield* idAllocator.allocate
            .plan({ threadId: turn.input.threadId, runId: turn.input.runId, driver: DRIVER })
            .pipe(Effect.mapError((cause) => protocolError("Cannot allocate a plan", cause))));
        turn.planId = planId;
        const base = baseItem(
          turn.scope,
          `${turnKey(turn)}:proposed-plan`,
          yield* DateTime.now,
          "completed",
        );
        yield* emit({
          type: "plan.updated",
          driver: DRIVER,
          plan: {
            id: planId,
            threadId: turn.input.threadId,
            runId: turn.input.runId,
            nodeId: base.nodeId,
            kind: "proposed_plan",
            status: "active",
            markdown,
          },
        });
        yield* publishItem(turn.scope, "plan", {
          ...base,
          type: "proposed_plan",
          planId,
          markdown,
          streaming: false,
        });
      });

      const showRequest = Effect.fnUntraced(function* (entry: PendingRequest) {
        yield* emit({
          type: "runtime_request.updated",
          driver: DRIVER,
          threadId: entry.turn.input.threadId,
          runtimeRequest: entry.request,
        });
        yield* emit({ type: "node.updated", driver: DRIVER, node: entry.node });
        yield* emit({ type: "turn_item.updated", driver: DRIVER, turnItem: entry.item });
      });
      const openRequest = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        nativeRequestId: string,
        native: PendingRequest["native"],
        card:
          | {
              readonly type: "approval";
              readonly kind: ProviderRequestKind;
              readonly prompt: string;
              readonly options: ReadonlyArray<ProviderApprovalOption>;
            }
          | { readonly type: "question"; readonly request: UserInputRequest },
      ) {
        const requestId = yield* idAllocator.allocate
          .runtimeRequest({
            driver: DRIVER,
            providerTurnId: turn.providerTurn.id,
            nativeRequestId,
          })
          .pipe(Effect.mapError((cause) => protocolError("Cannot allocate a request", cause)));
        const time = yield* DateTime.now;
        const nodeId = idAllocator.derive.approvalNode({ requestId });
        const request: OrchestrationV2RuntimeRequest = {
          id: requestId,
          nodeId,
          providerTurnId: turn.providerTurn.id,
          nativeRequestRef: nativeRef(
            nativeRequestId,
            native.type === "question" ? "weak" : "strong",
          ),
          kind: card.type === "approval" ? card.kind : "user_input",
          status: "pending",
          responseCapability: { type: "live", providerSessionId: input.providerSessionId },
          createdAt: time,
          resolvedAt: null,
        };
        const base = {
          ...baseItem(turn.scope, `request:${nativeRequestId}`, time, "waiting"),
          id: idAllocator.derive.approvalTurnItem({ requestId }),
          nodeId,
          nativeItemRef: request.nativeRequestRef,
        };
        const choices = card.type === "question" ? (card.request.choices ?? []) : [];
        const entry: PendingRequest = {
          turn,
          native,
          request,
          node: {
            id: nodeId,
            threadId: turn.input.threadId,
            runId: turn.input.runId,
            parentNodeId: turn.input.rootNodeId,
            rootNodeId: turn.input.rootNodeId,
            kind: card.type === "approval" ? "approval_request" : "user_input_request",
            status: "waiting",
            countsForRun: false,
            providerThreadId: turn.input.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            nativeItemRef: request.nativeRequestRef,
            runtimeRequestId: requestId,
            checkpointScopeId: null,
            startedAt: time,
            completedAt: null,
          },
          item:
            card.type === "approval"
              ? {
                  ...base,
                  type: "approval_request",
                  requestId,
                  requestKind: card.kind,
                  prompt: card.prompt,
                  options: card.options,
                }
              : {
                  ...base,
                  type: "user_input_request",
                  requestId,
                  questions: [
                    {
                      id: QUESTION_ID,
                      header: "Question",
                      question: card.request.question.trim() || "Copilot has a question",
                      options: choices
                        .filter((choice) => choice.trim().length > 0)
                        .map((choice) => ({ label: choice.trim(), description: choice.trim() })),
                      multiSelect: false,
                      allowCustomAnswer:
                        card.request.allowFreeform !== false || choices.length === 0,
                      required: true,
                    },
                  ],
                },
          answered: false,
        };
        pending.set(requestId, entry);
        if (native.type === "permission") pendingPermissions.set(native.requestId, requestId);
        yield* showRequest(entry);
        yield* updateSession("waiting");
      });
      const forgetRequest = Effect.fnUntraced(function* (entry: PendingRequest) {
        if (pending.get(entry.request.id) !== entry) return false;
        pending.delete(entry.request.id);
        if (entry.native.type === "permission") pendingPermissions.delete(entry.native.requestId);
        if (active === entry.turn && pending.size === 0) yield* updateSession("running");
        return true;
      });
      /** Closes a request Copilot stopped waiting on; one T3 answered keeps the answer T3 saved. */
      const closeRequest = Effect.fnUntraced(function* (
        entry: PendingRequest,
        outcome: "resolved" | "cancelled",
      ) {
        if (!(yield* forgetRequest(entry)) || entry.answered) return;
        const time = yield* DateTime.now;
        const status = outcome === "resolved" ? "completed" : "cancelled";
        entry.request = { ...entry.request, status: outcome, resolvedAt: time };
        entry.node = { ...entry.node, status, completedAt: time };
        entry.item = { ...entry.item, status, completedAt: time, updatedAt: time };
        yield* showRequest(entry);
      });
      const requestPermission = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        data: SessionEventPayload<"permission.requested">["data"],
      ) {
        if (data.resolvedByHook === true || pendingPermissions.has(data.requestId)) return;
        const target = session;
        if (!target) return;
        const request = data.permissionRequest;
        const policy = turn.input.runtimePolicy;
        // Copilot can clear the managed marker only on prompt kinds that carry it.
        const prompted = data.promptRequest;
        const needsHuman =
          (prompted !== undefined && "managedApprovalRequired" in prompted
            ? prompted.managedApprovalRequired
            : request.managedApprovalRequired) === true;
        const automatic =
          !needsHuman &&
          (currentSettings.allowAll ||
            request.kind === "read" ||
            (policy.runtimeMode === "auto-accept-edits" && request.kind === "write"));
        if (automatic) {
          // Forked: this runs under the event permit, which cleanup must always be able to take.
          return yield* call("automatic approval", () =>
            target.rpc.permissions.handlePendingPermissionRequest({
              requestId: data.requestId,
              result: { kind: "approve-once" },
            }),
          ).pipe(Effect.ignore, Effect.forkIn(sessionScope), Effect.asVoid);
        }
        const presentation = permissionPresentation(request);
        yield* openRequest(
          turn,
          data.requestId,
          { type: "permission", requestId: data.requestId, forSession: presentation.forSession },
          {
            type: "approval",
            kind: presentation.kind,
            prompt: presentation.prompt,
            options: [
              { decision: "accept", label: "Allow once" },
              ...(presentation.forSession === undefined
                ? []
                : [{ decision: "acceptForSession" as const, label: "Allow for this session" }]),
              { decision: "decline", label: "Deny" },
            ],
          },
        );
      });

      const finish = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        status: "completed" | "cancelled" | "interrupted" | "failed",
        detail?: string,
        disposition: "reusable" | "broken" = "reusable",
      ) {
        if (active !== turn) return;
        // session.idle waits for background agents, so a still-running one did not finish.
        for (const subagent of turn.subagents.values()) {
          yield* settleSubagent(turn, subagent, status === "completed" ? "interrupted" : status);
        }
        yield* settleScope(turn.scope, status, turn.subagents);
        const compaction = turn.compaction;
        turn.compaction = undefined;
        if (compaction !== undefined) {
          const time = yield* DateTime.now;
          yield* publishItem(turn.scope, "system", {
            ...compaction,
            status: status === "completed" ? "failed" : status,
            title:
              status === "completed" || status === "failed"
                ? "Context compaction failed"
                : "Context compaction stopped",
            completedAt: time,
            updatedAt: time,
          });
        }
        // T3 dismisses a turn's open questions itself when it ends, but not its approvals.
        for (const entry of pending.values()) {
          if (entry.turn !== turn) continue;
          if (entry.native.type === "permission") {
            yield* closeRequest(entry, "cancelled");
            continue;
          }
          if (!entry.answered) {
            entry.native.reject(new Error("The T3 Code turn ended before the user answered."));
          }
          yield* forgetRequest(entry);
        }
        const completedAt = yield* DateTime.now;
        const usage = turn.providerTurn.turnTokenUsage;
        turn.providerTurn = {
          ...turn.providerTurn,
          status,
          completedAt,
          ...(usage?.inputTokens === undefined || usage.outputTokens === undefined
            ? {}
            : {
                turnTokenUsage: {
                  ...usage,
                  hasSubagents: turn.subagents.size > 0,
                  usageStatus: "complete",
                  inputTokens: usage.inputTokens,
                  outputTokens: usage.outputTokens,
                },
              }),
        };
        yield* emitTurn(turn);
        yield* updateThread({ status: disposition === "broken" ? "error" : "idle" });
        yield* updateSession(
          disposition === "broken" ? "error" : "ready",
          status === "failed" ? (detail ?? null) : null,
        );
        active = undefined;
        if (status === "failed") {
          const failure = makeProviderFailure({
            // A turn that failed before its process went away keeps that failure's class, such as
            // a usage limit.
            class:
              disposition === "broken" && turn.lastError === undefined && turn.ending === undefined
                ? "transport_error"
                : failureClass(turn.lastError),
            message: detail ?? "Copilot could not finish the turn.",
            ...(turn.lastError?.errorCode === undefined ? {} : { code: turn.lastError.errorCode }),
          });
          // T3 builds the error row from the terminal; this only reserves its position.
          const base = baseItem(turn.scope, `${turnKey(turn)}:failure`, completedAt, "failed");
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

      /** Kills the CLI, then ends the turn as broken, so the next turn starts a fresh process. */
      const killTurn = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        status: "failed" | "cancelled" | "interrupted",
        detail?: string,
      ) {
        // A second stop, such as an interrupt's deadline during a release, waits for the first.
        if (turn.forcedStop) return yield* Deferred.await(turn.done);
        // forceStop rejects the request before the process is gone; forcedStop keeps that
        // rejection, and any event until the kill finishes, from ending the turn.
        turn.forcedStop = true;
        markBroken();
        const failure = yield* terminate(client);
        yield* Effect.gen(function* () {
          yield* finish(turn, status, detail, "broken");
          if (failure !== undefined) {
            yield* updateSession("error", `Copilot's processes may still be running (${failure})`);
          }
        }).pipe(eventPermit.withPermits(1));
      });

      /** Copilot left an RPC unanswered, so T3 stops it rather than wait, as for an interrupt. */
      const stopUnresponsive = Effect.fnUntraced(
        function* (
          detail: string,
          sender: { readonly turn: ActiveTurn | undefined; readonly client: CopilotSdkClient },
        ) {
          // An answer can outlive its turn; once Copilot moved past it, nothing is stuck.
          if (sender.client !== client || sender.turn !== active) {
            return yield* Effect.logWarning("Copilot never answered an earlier request", {
              detail,
            });
          }
          const turn = sender.turn;
          if (turn !== undefined) return yield* killTurn(turn, "failed", detail);
          // No turn ends, so the thread's error status makes T3 resume it on a new process.
          markBroken();
          const failure = yield* terminate(client);
          yield* Effect.gen(function* () {
            yield* updateThread({ status: "error" });
            yield* updateSession(
              "error",
              failure === undefined ? detail : `${detail} It may still be running (${failure}).`,
            );
          }).pipe(eventPermit.withPermits(1));
        },
        Effect.forkIn(sessionScope),
        Effect.asVoid,
      );
      /** Bounds an RPC; if Copilot never answers, T3 stops the turn and client that sent it. */
      const bounded = <A, E>(
        operation: string,
        deadline: Duration.Input,
        rpc: Effect.Effect<A, E>,
      ) =>
        Effect.suspend(() => {
          const sender = { turn: active, client };
          return rpc.pipe(
            Effect.timeoutOrElse({
              duration: deadline,
              orElse: () =>
                stopUnresponsive(`Copilot did not answer the ${operation} in time.`, sender).pipe(
                  Effect.andThen(
                    Effect.fail(protocolError(`Copilot did not answer the ${operation} in time`)),
                  ),
                ),
            }),
          );
        });
      /** Whether Copilot lists a running task; undefined when it cannot say in time. */
      const listsRunningTasks = (target: CopilotSdkSession) =>
        Effect.tryPromise(() => target.rpc.tasks.list()).pipe(
          Effect.timeoutOption(CLIENT_STOP_TIMEOUT),
          Effect.map((listed) =>
            Option.isSome(listed)
              ? listed.value.tasks.some((task) => task.status === "running")
              : undefined,
          ),
          Effect.orElseSucceed(() => undefined),
        );
      /**
       * Ends a turn that did not complete. Autopilot shells and background agents outlive the
       * request, so T3 takes the thread back only once Copilot lists none running; otherwise it
       * stops Copilot first.
       */
      const release = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        status: "failed" | "cancelled" | "interrupted",
        detail?: string,
      ) {
        const target = session;
        if (turn.mode !== "autopilot" || target === undefined) {
          return yield* finish(turn, status, detail);
        }
        if (turn.ending !== undefined) return;
        turn.ending = { status, ...(detail === undefined ? {} : { detail }) };
        turn.tasksChanged = false;
        yield* listsRunningTasks(target).pipe(
          Effect.flatMap((busy) => Queue.offer(inbox, { type: "ending", turn, busy })),
          Effect.forkIn(sessionScope),
        );
      });

      // Projection runs per SDK event inside the session's span; a span per event is noise.
      const projectContent = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        scope: ItemScope,
        event: SessionEvent,
      ) {
        switch (event.type) {
          case "assistant.message_delta":
            return yield* appendText(
              scope,
              `message:${event.data.messageId}`,
              "assistant_message",
              event.data.deltaContent,
            );
          case "assistant.message":
            return yield* completeText(
              scope,
              `message:${event.data.messageId}`,
              "assistant_message",
              event.data.content,
            );
          case "assistant.reasoning_delta":
            return yield* appendText(
              scope,
              `reasoning:${event.data.reasoningId}`,
              "reasoning",
              event.data.deltaContent,
            );
          case "assistant.reasoning":
            return yield* completeText(
              scope,
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
            scope.tools.set(tool.toolCallId, tool);
            if (tool.toolName === SUBAGENT_TOOL && tool.mcpServerName === undefined) {
              return yield* startSubagent(turn, scope, tool);
            }
            return yield* publishTool(scope, tool);
          }
          case "tool.shell_output": {
            const tool = scope.tools.get(event.data.toolCallId);
            if (!tool || tool.completion !== undefined) return;
            const time = yield* DateTime.now;
            return yield* emit({
              type: "command_output.delta",
              driver: DRIVER,
              threadId: scope.threadId,
              itemId: baseItem(scope, `tool:${tool.toolCallId}`, time, "running").id,
              chunk: event.data.text,
            });
          }
          case "tool.execution_complete": {
            const tool: ToolCall = scope.tools.get(event.data.toolCallId) ?? {
              toolCallId: event.data.toolCallId,
              toolName: event.data.toolDescription?.name ?? "tool",
              title: undefined,
              arguments: undefined,
              mcpServerName: undefined,
              mcpToolName: undefined,
              displayCommand: undefined,
            };
            tool.completion = event.data;
            scope.tools.set(tool.toolCallId, tool);
            const subagent = turn.subagents.get(tool.toolCallId);
            if (subagent === undefined) return yield* publishTool(scope, tool);
            // A background task's call settles at launch; subagent.completed ends it.
            if (!event.data.success) {
              return yield* settleSubagent(turn, subagent, "failed", event.data.error?.message);
            }
            if (!subagent.background) {
              return yield* settleSubagent(
                turn,
                subagent,
                "completed",
                event.data.result?.detailedContent ?? event.data.result?.content,
              );
            }
            return;
          }
        }
      });
      const handleEvent = Effect.fnUntraced(function* (event: SessionEvent) {
        const turn = active;
        // Once T3 kills the CLI, only the kill path ends the turn.
        if (!turn || turn.forcedStop) return;
        const root = event.agentId === undefined;
        switch (event.type) {
          case "subagent.started": {
            const subagent = turn.subagents.get(event.data.toolCallId);
            if (!subagent) return;
            if (event.agentId !== undefined) turn.agents.set(event.agentId, subagent);
            subagent.model = event.data.model ?? subagent.model;
            return yield* publishSubagent(subagent);
          }
          case "subagent.completed": {
            const subagent = turn.subagents.get(event.data.toolCallId);
            if (!subagent) return;
            return yield* settleSubagent(
              turn,
              subagent,
              event.data.cancelled === true ? "cancelled" : "completed",
            );
          }
          case "subagent.failed": {
            const subagent = turn.subagents.get(event.data.toolCallId);
            if (!subagent) return;
            return yield* settleSubagent(turn, subagent, "failed", event.data.error);
          }
          // Requests from subagents surface on the root turn, where the user answers them.
          case "permission.requested":
            return yield* requestPermission(turn, event.data);
          case "permission.completed": {
            const requestId = pendingPermissions.get(event.data.requestId);
            const entry = requestId === undefined ? undefined : pending.get(requestId);
            if (entry === undefined) return;
            return yield* closeRequest(
              entry,
              event.data.result.kind.startsWith("approved") ? "resolved" : "cancelled",
            );
          }
          case "assistant.usage": {
            // Turn usage is the main agent's; a child's calls are its own.
            if (!root || event.data.parentToolCallId !== undefined) return;
            const accounting = event.data.accounting;
            const usageId =
              accounting === undefined
                ? event.id
                : `${accounting.sourceSessionId}:${accounting.usageId}`;
            if (turn.usageIds.has(usageId)) return;
            turn.usageIds.add(usageId);
            const previous = turn.providerTurn.turnTokenUsage;
            const add = (total: number | undefined, value: number | undefined) =>
              total === undefined && value === undefined ? undefined : (total ?? 0) + (value ?? 0);
            const cacheCreationTokens = add(
              previous?.cacheCreationTokens,
              event.data.cacheWriteTokens,
            );
            turn.providerTurn = {
              ...turn.providerTurn,
              turnTokenUsage: {
                usageScope: "main_agent",
                usageStatus: "partial",
                hasSubagents: turn.subagents.size > 0,
                inputTokens: add(previous?.inputTokens, event.data.inputTokens) ?? 0,
                outputTokens: add(previous?.outputTokens, event.data.outputTokens) ?? 0,
                cachedInputTokens:
                  add(previous?.cachedInputTokens, event.data.cacheReadTokens) ?? 0,
                reasoningTokens: add(previous?.reasoningTokens, event.data.reasoningTokens) ?? 0,
                ...(cacheCreationTokens === undefined ? {} : { cacheCreationTokens }),
              },
            };
            return yield* emitTurn(turn);
          }
          case "session.usage_info": {
            if (!root) return;
            const updatedAt = DateTime.formatIso(yield* DateTime.now);
            yield* updateThread({
              contextUsage: {
                usedTokens: event.data.currentTokens,
                ...(event.data.tokenLimit > 0 ? { maxTokens: event.data.tokenLimit } : {}),
              },
            });
            turn.providerTurn = {
              ...turn.providerTurn,
              tokenUsage: {
                ...turn.providerTurn.tokenUsage,
                usedTokens: event.data.currentTokens,
                maxTokens: event.data.tokenLimit,
                updatedAt,
              },
            };
            return yield* emitTurn(turn);
          }
          case "session.compaction_start": {
            if (!root) return;
            const key =
              event.data.compactionAttemptId === undefined
                ? `${turnKey(turn)}:compaction:${turn.scope.ordinals.size}`
                : `compaction:${event.data.compactionAttemptId}`;
            turn.compaction = {
              ...baseItem(turn.scope, key, yield* DateTime.now, "running"),
              type: "compaction",
              driver: DRIVER,
              ...(event.data.currentTokens === undefined
                ? {}
                : { beforeTokenCount: event.data.currentTokens }),
            };
            return yield* publishItem(turn.scope, "system", turn.compaction);
          }
          case "session.compaction_complete": {
            if (!root) return;
            const key =
              event.data.compactionAttemptId === undefined
                ? (turn.compaction?.nativeItemRef?.nativeId ??
                  `${turnKey(turn)}:compaction:${turn.scope.ordinals.size}`)
                : `compaction:${event.data.compactionAttemptId}`;
            turn.compaction = undefined;
            const summary = event.data.summaryContent?.trim();
            return yield* publishItem(turn.scope, "system", {
              ...baseItem(
                turn.scope,
                key,
                yield* DateTime.now,
                event.data.success ? "completed" : "failed",
              ),
              type: "compaction",
              driver: DRIVER,
              ...(summary ? { summary } : {}),
              ...(event.data.preCompactionTokens === undefined
                ? {}
                : { beforeTokenCount: event.data.preCompactionTokens }),
              ...(event.data.postCompactionTokens === undefined
                ? {}
                : { afterTokenCount: event.data.postCompactionTokens }),
            });
          }
          case "session.error": {
            if (!root) return;
            // Copilot refused a steer before running it; the turn goes on without it.
            const refused = event.data.originatingMessageId;
            if (refused !== undefined && turn.steers.delete(refused)) {
              yield* publishItem(turn.scope, "system", {
                ...baseItem(turn.scope, `error:${event.id}`, yield* DateTime.now, "completed"),
                type: "system_notice",
                title: "Copilot did not run a steering message",
                message: event.data.message,
              });
              return yield* complete(turn);
            }
            turn.lastError = event.data;
            // After the request settled, nothing else reports a follow-up run's failure.
            if (turn.requestSettled) return yield* release(turn, "failed", event.data.message);
            // A steer runs on its own only once the request's run idled, so this error is the
            // follow-up's; the request can still settle successfully after it.
            if (turn.followUpRunning) {
              turn.followUpFailure = event.data;
              turn.followUpRunning = false;
            }
            return;
          }
          case "session.warning": {
            if (!root) return;
            const time = yield* DateTime.now;
            yield* publishItem(turn.scope, "system", {
              ...baseItem(turn.scope, `warning:${event.id}`, time, "completed"),
              type: "system_notice",
              title: "Copilot warning",
              message: event.data.message,
            });
            // A policy-blocked prompt gets this warning instead of a user.message.
            const blocked = event.data.originatingMessageId;
            if (blocked !== undefined && turn.steers.delete(blocked)) return yield* complete(turn);
            return;
          }
          case "session.background_tasks_changed":
            if (!root) return;
            if (turn.draining || turn.ending !== undefined) turn.tasksChanged = true;
            if (!turn.drainWaiting) return;
            turn.drainWaiting = false;
            return yield* complete(turn);
          case "user.message":
            if (!root || event.data.messageId === undefined) return;
            if (!turn.steers.delete(event.data.messageId)) return;
            // "steering" joined the running request; "idle" or "queued" started a run of its own.
            if (event.data.delivery !== undefined && event.data.delivery !== "steering") {
              turn.followUpRunning = true;
            }
            return;
          case "assistant.turn_start":
            // After the request settled, a root run is Copilot's own follow-up, such as one
            // answering a finished background task. It ends as a steer's run does.
            if (root && turn.requestSettled && event.data.parentToolCallId === undefined) {
              turn.followUpRunning = true;
            }
            return;
          case "session.idle":
            // Autopilot idles between continuations; autopilot_stopped ends its runs, as the
            // SDK's own sendAndWait decides.
            if (!root || event.data.mode === "autopilot") return;
            turn.followUpRunning = false;
            if (turn.requestSettled && event.data.aborted === true) {
              return yield* finish(turn, turn.interruptRequested ? "interrupted" : "cancelled");
            }
            return yield* complete(turn);
          case "session.autopilot_stopped":
            if (!root) return;
            turn.followUpRunning = false;
            if (!turn.requestSettled) return;
            if (event.data.reason === "error") {
              return yield* release(turn, "failed", turn.lastError?.message);
            }
            if (event.data.reason === "aborted") {
              return yield* release(turn, turn.interruptRequested ? "interrupted" : "cancelled");
            }
            return yield* complete(turn);
        }
        const agentId = event.agentId;
        if (agentId === undefined) return yield* projectContent(turn, turn.scope, event);
        // Copilot tags a child's events with its agentId; older events also name the task call.
        const parentToolCallId = stringField(event.data, "parentToolCallId");
        const subagent =
          turn.agents.get(agentId) ??
          (parentToolCallId === undefined ? undefined : turn.subagents.get(parentToolCallId));
        if (!subagent) return;
        turn.agents.set(agentId, subagent);
        yield* projectContent(turn, subagent.scope, event);
      });
      /** Ends a settled turn once Copilot owes nothing for it. */
      const complete = Effect.fnUntraced(function* (turn: ActiveTurn) {
        // An ending turn has its own task check, and its task-change marker must survive.
        if (turn.ending !== undefined) return;
        if (!turn.requestSettled || turn.steers.size > 0 || turn.followUpRunning) return;
        if (turn.followUpFailure !== undefined) {
          return yield* release(turn, "failed", turn.followUpFailure.message);
        }
        if (turn.mode !== "autopilot") return yield* finish(turn, "completed");
        if (turn.draining) return;
        // Autopilot completion does not wait for attached shells or background agents.
        turn.draining = true;
        turn.drainWaiting = false;
        turn.tasksChanged = false;
        const target = session;
        if (target === undefined) return yield* finish(turn, "completed");
        // waitForPending also returns on its own timeout, so the task list decides.
        yield* Effect.tryPromise(() => target.rpc.tasks.waitForPending()).pipe(
          Effect.timeout(BACKGROUND_WAIT_TIMEOUT),
          Effect.catchCause((cause) =>
            Effect.logWarning("Could not wait for Copilot's background work", cause),
          ),
          Effect.andThen(listsRunningTasks(target)),
          Effect.flatMap((busy) => Queue.offer(inbox, { type: "drained", turn, busy })),
          Effect.forkIn(sessionScope),
        );
      });
      const settle = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        exit: Exit.Exit<unknown, ProviderAdapter.ProviderAdapterProtocolError>,
      ) {
        // forceStop rejects the request before the process is gone; the kill path ends the turn.
        if (turn.forcedStop) return;
        if (Exit.isSuccess(exit)) {
          turn.requestSettled = true;
          return yield* complete(turn);
        }
        if (turn.interruptRequested) return yield* release(turn, "interrupted");
        const failure = Cause.squash(exit.cause);
        const reason = errorText(isProtocolError(failure) ? failure.cause : failure);
        if (/\bdisconnected\b/i.test(reason ?? "")) {
          markBroken();
          return yield* finish(
            turn,
            "failed",
            turn.lastError?.message ?? "Copilot stopped unexpectedly.",
            "broken",
          );
        }
        if (/\baborted\b/i.test(reason ?? "")) return yield* release(turn, "cancelled");
        yield* release(turn, "failed", turn.lastError?.message ?? reason);
      });
      const handleInbox = Effect.fnUntraced(
        function* (entry: Inbox) {
          switch (entry.type) {
            case "event":
              if (entry.epoch === sessionEpoch) yield* handleEvent(entry.event);
              return;
            case "flush":
              entry.scope.flushScheduled = false;
              for (const key of entry.scope.dirty) yield* publishText(entry.scope, key);
              return;
            case "settled":
              if (active === entry.turn) yield* settle(entry.turn, entry.exit);
              return;
            case "ending": {
              const turn = entry.turn;
              const ending = turn.ending;
              if (active !== turn || turn.forcedStop || ending === undefined) return;
              // A steer's run or a task that started after the list may still be working.
              const quiet =
                entry.busy === false &&
                turn.steers.size === 0 &&
                !turn.followUpRunning &&
                !turn.tasksChanged;
              if (quiet) return yield* finish(turn, ending.status, ending.detail);
              // Forked: the kill path waits for this event permit.
              return yield* killTurn(turn, ending.status, ending.detail).pipe(
                Effect.forkIn(sessionScope),
                Effect.asVoid,
              );
            }
            case "drained": {
              const turn = entry.turn;
              // An ending turn waits for its own task list; this one may predate its last work.
              if (active !== turn || turn.forcedStop || turn.ending !== undefined) return;
              turn.draining = false;
              // A steer may have started a run while the drain waited; that run ends the turn.
              if (turn.steers.size > 0 || turn.followUpRunning) return;
              // Work can start after Copilot listed the tasks; only a fresh drain can end the turn.
              if (turn.tasksChanged) return yield* complete(turn);
              if (entry.busy === undefined) {
                if (++turn.listFailures < DRAIN_LIST_ATTEMPTS) return yield* complete(turn);
                // Its shells or agents may still be editing files, so T3 stops Copilot before it
                // takes the thread back. Forked: the kill path waits for this event permit.
                return yield* killTurn(
                  turn,
                  "failed",
                  "Copilot did not report whether its background work finished.",
                ).pipe(Effect.forkIn(sessionScope), Effect.asVoid);
              }
              turn.listFailures = 0;
              if (!entry.busy) return yield* finish(turn, "completed");
              turn.drainWaiting = true;
              return;
            }
            case "compacted": {
              const turn = entry.turn;
              if (active !== turn || turn.forcedStop) return;
              if (Exit.isFailure(entry.exit)) return yield* settle(turn, entry.exit);
              const result = entry.exit.value;
              // Copilot reports the compaction through its events; this settles one they left open.
              const compaction = turn.compaction;
              turn.compaction = undefined;
              if (compaction !== undefined) {
                const time = yield* DateTime.now;
                const summary = result.summaryContent?.trim();
                const afterTokenCount = result.contextWindow?.currentTokens;
                yield* publishItem(turn.scope, "system", {
                  ...compaction,
                  status: result.success ? "completed" : "failed",
                  ...(summary ? { summary } : {}),
                  ...(afterTokenCount === undefined ? {} : { afterTokenCount }),
                  completedAt: time,
                  updatedAt: time,
                });
              }
              if (result.success) return yield* settle(turn, Exit.void);
              if (turn.interruptRequested) return yield* finish(turn, "interrupted");
              return yield* finish(
                turn,
                "failed",
                turn.lastError?.message ?? "Copilot could not compact the conversation.",
              );
            }
            case "exited": {
              if (entry.client !== client || closed) return;
              markBroken();
              const turn = active;
              if (turn?.forcedStop) return;
              if (turn) {
                return yield* finish(
                  turn,
                  "failed",
                  turn.lastError?.message ?? "Copilot stopped unexpectedly.",
                  "broken",
                );
              }
              // An error thread makes T3 resume it, which relaunches Copilot.
              yield* updateThread({ status: "error" });
              yield* updateSession("error", "Copilot stopped unexpectedly.");
              return;
            }
            case "plan":
              if (active) yield* publishPlan(active, entry.markdown);
              return;
            case "question": {
              const turn = active;
              if (!turn) {
                return entry.reject(new Error("Copilot asked a question outside a T3 Code turn."));
              }
              const nativeId = `${turnKey(turn)}:ask_user:${++questionCount}`;
              return yield* openRequest(
                turn,
                nativeId,
                {
                  type: "question",
                  request: entry.request,
                  resolve: entry.resolve,
                  reject: entry.reject,
                },
                { type: "question", request: entry.request },
              );
            }
          }
        },
        Effect.catchCause((cause) =>
          Effect.logWarning("Could not project a Copilot SDK event", cause),
        ),
        eventPermit.withPermits(1),
      );
      yield* Stream.fromQueue(inbox).pipe(
        Stream.runForEach(handleInbox),
        Effect.forkIn(sessionScope),
      );
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          closed = true;
          sessionEpoch++;
          if (active) yield* finish(active, "cancelled", "Copilot session closed", "broken");
          yield* Queue.shutdown(inbox);
          yield* Queue.shutdown(events);
        }).pipe(eventPermit.withPermits(1)),
      );

      // The SDK hands this handler no request id, so the question gets a local one.
      const onUserInputRequest: UserInputHandler = (request) =>
        new Promise((resolve, reject) => {
          Queue.offerUnsafe(inbox, { type: "question", request, resolve, reject });
        });
      // T3 owns the plan review; Copilot is told to stop, as Claude is for ExitPlanMode.
      const onExitPlanModeRequest: NonNullable<SessionConfig["onExitPlanModeRequest"]> = (
        request,
      ) => {
        Queue.offerUnsafe(inbox, {
          type: "plan",
          markdown: request.planContent?.trim() || request.summary,
        });
        return { approved: false, feedback: PLAN_CAPTURED_FEEDBACK };
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
        // A replaced session keeps the app thread's row and, unless forced new, its conversation.
        const previous = thread?.appThreadId === args.threadId ? thread : undefined;
        if (args.forceNewNativeThread && session) {
          const replaced = session;
          session = undefined;
          sessionEpoch++;
          // Dropped either way; a silent CLI is caught by the session open that follows.
          yield* Effect.tryPromise(() => replaced.disconnect()).pipe(
            Effect.timeoutOption(RPC_TIMEOUT),
            Effect.ignore,
          );
        }
        if (broken) {
          const failure = yield* terminate(client);
          // A second CLI would write to the same workspace as the one that would not stop.
          const alive =
            client.process === undefined ? failure !== undefined : !client.process.hasExited();
          if (alive) {
            const pid = client.process === undefined ? "" : ` (process ${client.process.id})`;
            const detail = `Copilot${pid} is still running after T3 tried to stop it. End it, then try again.`;
            yield* updateSession("error", detail).pipe(eventPermit.withPermits(1));
            return yield* protocolError(detail);
          }
          client = yield* launchClient;
          broken = false;
          yield* observeExit(client);
        }
        const requestedId = args.forceNewNativeThread
          ? undefined
          : (existing?.nativeThreadRef?.nativeId ??
            previous?.nativeThreadRef?.nativeId ??
            input.initialNativeThreadId);
        if (
          previous &&
          session &&
          (requestedId === undefined || requestedId === session.sessionId)
        ) {
          return previous;
        }
        if (session) {
          return yield* protocolError("Open a new Copilot session to attach another thread");
        }
        const mcp = yield* mcpSessions.read(args.threadId);
        const settings = copilotSessionSettings(args.modelSelection, args.runtimePolicy);
        const epoch = ++sessionEpoch;
        // Without onPermissionRequest Copilot leaves requests pending as events with their ids.
        const config = {
          clientName: "t3-code",
          model: settings.model,
          ...(settings.reasoningEffort === undefined
            ? {}
            : { reasoningEffort: settings.reasoningEffort }),
          workingDirectory: cwd,
          streaming: true,
          contextTier: CONTEXT_TIER,
          // The CLI finds personal and project skills on its own; an SDK session only when asked.
          // The config passed here still wins over what it discovers.
          enableConfigDiscovery: true,
          systemMessage: systemMessage(mcp),
          ...(mcp === undefined
            ? {}
            : { defaultAgent: { excludedTools: [...COPILOT_DELEGATION_TOOLS] } }),
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
          onUserInputRequest,
          onExitPlanModeRequest,
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
        }).pipe((open) =>
          bounded(
            requestedId === undefined ? "new session" : "session resume",
            SESSION_OPEN_TIMEOUT,
            open,
          ),
        );
        session = opened;
        currentSettings = settings;
        // A resumed session may hold an older effort override; its first turn sets both again.
        applied =
          requestedId === undefined
            ? { model: settings.model, reasoningEffort: settings.reasoningEffort }
            : {};
        const createdAt = yield* DateTime.now;
        const base = existing ?? previous;
        thread = base
          ? {
              ...base,
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
        bounded(
          operation,
          RPC_TIMEOUT,
          Effect.tryPromise({
            try: run,
            catch: (cause) => protocolError(`Copilot ${operation} failed`, cause),
          }),
        );
      const applySettings = Effect.fnUntraced(function* (
        target: CopilotSdkSession,
        settings: CopilotSessionSettings,
      ) {
        if (
          applied.model !== settings.model ||
          applied.reasoningEffort !== settings.reasoningEffort
        ) {
          // null clears an effort override; an omitted effort would keep the previous one.
          const switched = yield* call("model switch", () =>
            target.rpc.model.switchTo({
              modelId: settings.model,
              reasoningEffort: settings.reasoningEffort ?? null,
              contextTier: CONTEXT_TIER,
            }),
          );
          // A confirmation means Copilot did not switch: the target model's context is too small.
          const confirmation = switched.confirmation;
          if (confirmation !== undefined) {
            applied = {};
            return yield* new CopilotModelSwitchError({
              model: confirmation.targetModelDisplayName,
              currentTokens: confirmation.currentTokens,
              targetLimit: confirmation.targetLimit,
            });
          }
          // A deferred switch is queued ahead of the prompt but not applied yet.
          if (switched.deferred === true) {
            yield* Effect.logWarning("Copilot queued the model switch instead of applying it", {
              model: settings.model,
            });
            applied = { ...applied, model: undefined, reasoningEffort: undefined };
          } else {
            applied = {
              ...applied,
              model: settings.model,
              reasoningEffort: settings.reasoningEffort,
            };
          }
        }
        if (applied.mode !== settings.mode) {
          yield* call("mode switch", () => target.rpc.mode.set({ mode: settings.mode }));
        }
        if (applied.allowAll !== settings.allowAll) {
          yield* call("permission mode switch", () =>
            target.rpc.permissions.setMode({ mode: settings.allowAll ? "allow-all" : "manual" }),
          );
        }
        applied = { ...applied, mode: settings.mode, allowAll: settings.allowAll };
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

      /** Makes the input T3's active turn on this session; the caller then sends its request. */
      const beginTurn = Effect.fnUntraced(function* (
        target: CopilotSdkSession,
        turnInput: ProviderAdapter.ProviderAdapterV2TurnInput,
        mode: CopilotMode,
      ) {
        // Short native ids, as ACP uses: T3's run-attempt ids would nest into every item id.
        // Instances can resume the same Copilot session; ids are global, so they carry the instance.
        const nativeTurnId = `${options.instanceId}:${target.sessionId}:turn:${turnInput.providerTurnOrdinal}`;
        const startedAt = yield* DateTime.now;
        const providerTurnId = idAllocator.derive.providerTurn({ driver: DRIVER, nativeTurnId });
        const turn: ActiveTurn = {
          input: turnInput,
          providerTurn: {
            id: providerTurnId,
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
          mode,
          scope: makeScope({
            threadId: turnInput.threadId,
            runId: turnInput.runId,
            providerThreadId: turnInput.providerThread.id,
            providerTurnId,
            rootNodeId: turnInput.rootNodeId,
            sharesRootNode: false,
            identity: `${options.instanceId}:${target.sessionId}`,
            ordinalBase: turnInput.providerTurnOrdinal * 100,
            appThread: turnInput.appThread,
            modelSelection: turnInput.modelSelection,
            mcpThreadId: turnInput.threadId,
          }),
          subagents: new Map(),
          agents: new Map(),
          usageIds: new Set(),
          done: yield* Deferred.make<void>(),
          planId: undefined,
          compaction: undefined,
          steers: new Set(),
          followUpRunning: false,
          requestSettled: false,
          followUpFailure: undefined,
          draining: false,
          drainWaiting: false,
          tasksChanged: false,
          listFailures: 0,
          ending: undefined,
          forcedStop: false,
          interruptRequested: false,
          lastError: undefined,
        };
        yield* Effect.gen(function* () {
          thread = turnInput.providerThread;
          active = turn;
          yield* emitTurn(turn);
          yield* updateThread({
            status: "active",
            firstRunOrdinal: thread.firstRunOrdinal ?? turnInput.runOrdinal,
            lastRunOrdinal: turnInput.runOrdinal,
          });
          yield* updateSession("running");
        }).pipe(eventPermit.withPermits(1));
        return turn;
      });
      const idleSession = Effect.fnUntraced(function* (
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
        return target;
      });

      const start = Effect.fnUntraced(function* (
        turnInput: ProviderAdapter.ProviderAdapterV2TurnInput,
      ) {
        const target = yield* idleSession(turnInput);
        const message = yield* prompt(turnInput.message);
        const settings = copilotSessionSettings(turnInput.modelSelection, turnInput.runtimePolicy);
        yield* applySettings(target, settings);
        const turn = yield* beginTurn(target, turnInput, settings.mode);
        // sendAndWait correlates completion with this request, waits out autopilot and background
        // agents, and rejects when the CLI exits. The SDK resolves it after dispatching the
        // completion event, so offering from its callbacks keeps the outcome right behind it.
        void target.sendAndWait(message, TURN_COMPLETION_TIMEOUT_MS).then(
          () => Queue.offerUnsafe(inbox, { type: "settled", turn, exit: Exit.void }),
          (cause: unknown) =>
            Queue.offerUnsafe(inbox, {
              type: "settled",
              turn,
              exit: Exit.fail(protocolError("Copilot could not finish the turn", cause)),
            }),
        );
      });
      // The turn's model is not applied first: a switch Copilot refused for a too-large context
      // is the reason to compact, and the compaction runs on the current model.
      const compact = Effect.fnUntraced(function* (
        turnInput: ProviderAdapter.ProviderAdapterV2TurnInput,
      ) {
        const target = yield* idleSession(turnInput);
        const turn = yield* beginTurn(target, turnInput, "interactive");
        void target.rpc.history.compact({ trigger: "manual" }).then(
          (result) =>
            Queue.offerUnsafe(inbox, { type: "compacted", turn, exit: Exit.succeed(result) }),
          (cause: unknown) =>
            Queue.offerUnsafe(inbox, {
              type: "compacted",
              turn,
              exit: Exit.fail(protocolError("Copilot could not compact the conversation", cause)),
            }),
        );
      });

      const answer = Effect.fn("CopilotSdkAdapter.respondToRuntimeRequest")(function* (
        args: ProviderAdapter.ProviderAdapterV2RuntimeRequestResponseInput,
      ) {
        const entry = pending.get(args.requestId);
        const target = session;
        if (!entry || !target) return yield* protocolError("This Copilot request is not pending");
        if (entry.native.type === "question") {
          // The legacy ask_user callback has no decline result; an answer saying so unblocks it.
          if (args.decision === "cancel" || args.decision === "decline") {
            entry.answered = true;
            entry.native.resolve({ answer: QUESTION_DECLINED, wasFreeform: true });
            return yield* forgetRequest(entry).pipe(eventPermit.withPermits(1), Effect.asVoid);
          }
          const raw = args.answers?.[QUESTION_ID];
          const text = typeof raw === "string" ? raw : Array.isArray(raw) ? raw[0] : undefined;
          if (typeof text !== "string" || text.trim().length === 0) {
            return yield* protocolError("Copilot needs a non-empty answer");
          }
          entry.answered = true;
          entry.native.resolve({
            answer: text,
            wasFreeform: !(entry.native.request.choices ?? []).includes(text),
          });
          return yield* forgetRequest(entry).pipe(eventPermit.withPermits(1), Effect.asVoid);
        }
        const decision = args.decision ?? "decline";
        const result: PermissionDecision =
          decision === "accept"
            ? { kind: "approve-once" }
            : decision === "acceptForSession" || decision === "acceptAlways"
              ? (entry.native.forSession ?? { kind: "approve-once" })
              : {
                  kind: "reject",
                  ...(args.feedback?.trim() ? { feedback: args.feedback.trim() } : {}),
                };
        const requestId = entry.native.requestId;
        // T3 saved the answer before delivering it, even if this response fails. Set first:
        // Copilot's permission.completed can arrive before the response returns.
        entry.answered = true;
        yield* call("permission response", () =>
          target.rpc.permissions.handlePendingPermissionRequest({ requestId, result }),
        );
        yield* forgetRequest(entry).pipe(eventPermit.withPermits(1));
      });

      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver: DRIVER,
        providerSessionId: input.providerSessionId,
        providerSession,
        events: Stream.fromQueue(events),
        // Copilot's background agents run inside its process and end with it.
        backgroundTasksEndWithProcess: true,
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
            // start() hands the prompt over last, so every failure it raises came before.
            Effect.mapError((cause) => new CopilotPromptNotSentError({ cause })),
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
        compactThread: (args) =>
          compact(args).pipe(
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
            // The event permit keeps the turn from settling during delivery, so the recorded id
            // tells settle whether Copilot owes this steer a run. Not the commands lock: Stop must
            // still run while a send hangs, and its kill rejects that send.
            yield* Effect.gen(function* () {
              if (active !== turn || session !== target || turn.interruptRequested) {
                return yield* protocolError("The Copilot turn ended before the steer was sent");
              }
              // Immediate delivery joins the running turn and leaves background agents running.
              // Unanswered, Copilot may still have taken it, and its run could never be matched to
              // this turn; call's timeout stops Copilot, so no work is left behind.
              const messageId = yield* call("steering message", () =>
                target.send({ ...message, mode: "immediate" }),
              );
              turn.steers.add(messageId);
            }).pipe(eventPermit.withPermits(1));
          },
          (effect, args) =>
            effect.pipe(
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
            // abort() only acknowledges; the turn ends when its request settles. One deadline
            // bounds both, since either can hang.
            const confirmed = yield* Effect.tryPromise(() => target.abort()).pipe(
              Effect.ignore,
              Effect.andThen(Deferred.await(turn.done)),
              Effect.timeoutOption(INTERRUPT_CONFIRMATION_TIMEOUT),
            );
            if (Option.isSome(confirmed)) return;
            // T3 checkpoints files after an interrupted turn, so the CLI must stop writing first.
            yield* killTurn(turn, "interrupted");
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
          answer(args).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                  driver: DRIVER,
                  requestId: args.requestId,
                  cause,
                }),
            ),
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
