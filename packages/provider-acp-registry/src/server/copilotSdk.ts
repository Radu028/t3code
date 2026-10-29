// @effect-diagnostics nodeBuiltinImport:off -- Windows path math must run on any host for the shim lookup, and the SDK's child is a Node ChildProcess.
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
    readonly tasks: Pick<CopilotSession["rpc"]["tasks"], "waitForPending">;
  };
}

export interface CopilotSdkClient {
  readonly createSession: (config: SessionConfig) => Promise<CopilotSdkSession>;
  readonly resumeSession: (
    sessionId: string,
    config: ResumeSessionConfig,
  ) => Promise<CopilotSdkSession>;
  readonly stop: () => Promise<ReadonlyArray<Error>>;
  readonly forceStop: () => Promise<void>;
  /** The Copilot process, so T3 can see it exit and stop its whole process tree. */
  readonly process: { readonly id: number; readonly exited: Promise<void> } | undefined;
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
  await client.start();
  // SDK 1.0.28 has no exit notification or pid accessor; its spawned child is a private field.
  const child: unknown = Reflect.get(client, "cliProcess");
  return {
    createSession: (config) => client.createSession(config),
    resumeSession: (sessionId, config) => client.resumeSession(sessionId, config),
    stop: () => client.stop(),
    forceStop: () => client.forceStop(),
    process:
      child instanceof NodeChildProcess.ChildProcess && child.pid !== undefined
        ? {
            id: child.pid,
            exited:
              child.exitCode !== null || child.signalCode !== null
                ? Promise.resolve()
                : new Promise((resolve) => child.once("exit", () => resolve())),
          }
        : undefined,
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
