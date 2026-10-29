// @effect-diagnostics nodeBuiltinImport:off globalTimers:off -- Windows path math must run on any host for the shim lookup, and the SDK's child is a Node ChildProcess. The SDK seam is Promise-based, outside an Effect runtime.
import * as NodeChildProcess from "node:child_process";
import * as NodePath from "node:path";
import type {
  CopilotSession,
  MessageOptions,
  ResumeSessionConfig,
  SessionConfig,
} from "@github/copilot-sdk";

/** The part of a Copilot SDK session the adapter drives; tests replace it with a fake. */
export interface CopilotSdkSession {
  readonly sessionId: string;
  readonly send: (options: MessageOptions) => Promise<string>;
  readonly sendAndWait: (options: MessageOptions, timeout?: number) => Promise<unknown>;
  readonly abort: () => Promise<void>;
  readonly disconnect: () => Promise<void>;
  readonly rpc: {
    // switchTo, not setModel: only switchTo can clear an effort override (null).
    readonly model: Pick<CopilotSession["rpc"]["model"], "switchTo">;
    readonly mode: Pick<CopilotSession["rpc"]["mode"], "set">;
    readonly permissions: Pick<
      CopilotSession["rpc"]["permissions"],
      "setMode" | "handlePendingPermissionRequest"
    >;
    readonly tasks: Pick<CopilotSession["rpc"]["tasks"], "waitForPending" | "list">;
    readonly history: Pick<CopilotSession["rpc"]["history"], "compact">;
  };
}

export interface CopilotSdkClient {
  /** Spawns the CLI and runs the SDK's handshake with it. */
  readonly start: () => Promise<void>;
  readonly createSession: (config: SessionConfig) => Promise<CopilotSdkSession>;
  readonly resumeSession: (
    sessionId: string,
    config: ResumeSessionConfig,
  ) => Promise<CopilotSdkSession>;
  readonly stop: () => Promise<ReadonlyArray<Error>>;
  readonly forceStop: () => Promise<void>;
  /**
   * The Copilot process, so T3 can see it exit and stop its whole process tree. Undefined until
   * start() spawned it; once read, kept, since a failed stop() clears the SDK's own field.
   */
  readonly process: CopilotProcess | undefined;
}

export interface CopilotProcess {
  readonly id: number;
  /** True once the process exited; its pid may then belong to another process. */
  readonly hasExited: () => boolean;
  /** Resolves after the exit and after the SDK dispatched every notification it had read. */
  readonly exited: Promise<void>;
  /** SIGKILLs the process directly, for when the SDK no longer tracks it. */
  readonly kill: () => void;
}

// A grandchild holding an inherited pipe can keep stdio open long after the exit.
const STDIO_CLOSE_TIMEOUT_MS = 5_000;

/**
 * Tracks the SDK client's CLI child. Its exit alone is too early: notifications can still sit
 * in stdout or in the connection's queue, so this waits for stdio to close, then for the
 * connection's drain, the barrier the SDK itself awaits before it marks sessions disconnected.
 */
export function copilotClientProcess(client: object): CopilotProcess | undefined {
  // SDK 1.0.28 has no exit notification, pid accessor or drain hook; its CLI child and its
  // connection (with a memoized drain()) are private fields.
  const child: unknown = Reflect.get(client, "cliProcess");
  const connection: unknown = Reflect.get(client, "connection");
  const drain: unknown =
    typeof connection === "object" && connection !== null
      ? Reflect.get(connection, "drain")
      : undefined;
  if (!(child instanceof NodeChildProcess.ChildProcess) || child.pid === undefined) {
    return undefined;
  }
  const hasExited = () => child.exitCode !== null || child.signalCode !== null;
  const stdioClosed = new Promise<void>((resolve) => {
    if (hasExited()) return resolve();
    child.once("close", () => resolve());
    child.once("exit", () => setTimeout(resolve, STDIO_CLOSE_TIMEOUT_MS).unref());
  });
  return {
    id: child.pid,
    hasExited,
    kill: () => void child.kill("SIGKILL"),
    exited: stdioClosed
      .then(() => (typeof drain === "function" ? Reflect.apply(drain, connection, []) : undefined))
      .then(
        () => undefined,
        () => undefined,
      ),
  };
}

export interface CopilotSdkClientOptions {
  readonly cliPath: string;
  readonly cwd: string;
  /** Replaces the inherited environment of the Copilot process. */
  readonly env: Readonly<Record<string, string>>;
}

export type MakeCopilotSdkClient = (options: CopilotSdkClientOptions) => Promise<CopilotSdkClient>;

// Loaded on first use so servers without T3_COPILOT_SDK never evaluate the SDK.
export const makeCopilotSdkClient: MakeCopilotSdkClient = async (options) => {
  const { CopilotClient, RuntimeConnection } = await import("@github/copilot-sdk");
  // An explicit stdio connection keeps the SDK off its koffi-backed in-process transport.
  const client = new CopilotClient({
    connection: RuntimeConnection.forStdio({ path: options.cliPath, env: { ...options.env } }),
    workingDirectory: options.cwd,
    useLoggedInUser: true,
  });
  let tracked: CopilotProcess | undefined;
  return {
    start: () => client.start(),
    createSession: (config) => client.createSession(config),
    resumeSession: (sessionId, config) => client.resumeSession(sessionId, config),
    stop: () => client.stop(),
    forceStop: () => client.forceStop(),
    get process() {
      tracked ??= copilotClientProcess(client);
      return tracked;
    },
  };
};

const WINDOWS_SHIM_EXTENSIONS = new Set([".cmd", ".bat", ".ps1", ""]);

/**
 * Executables to try, in order, for the command the ACP Registry resolved. The SDK spawns
 * without a shell, so an npm Windows shim maps to the Copilot binary it launches, then to
 * the loader script the SDK runs with Node.
 */
export function copilotSdkCliCandidates(input: {
  readonly command: string;
  readonly platform: NodeJS.Platform;
  readonly architecture: NodeJS.Architecture;
}): ReadonlyArray<string> {
  const path = NodePath.win32;
  if (
    input.platform !== "win32" ||
    !WINDOWS_SHIM_EXTENSIONS.has(path.extname(input.command).toLowerCase())
  ) {
    return [input.command];
  }
  const scope = path.join(path.dirname(input.command), "node_modules", "@github");
  const binary = path.join(`copilot-win32-${input.architecture}`, "copilot.exe");
  return [
    path.join(scope, "copilot", "node_modules", "@github", binary),
    path.join(scope, binary),
    path.join(scope, "copilot", "npm-loader.js"),
  ];
}
