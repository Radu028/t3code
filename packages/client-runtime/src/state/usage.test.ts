import {
  EnvironmentId,
  UsageDay,
  UsageLimitSourceId,
  USAGE_CONTRACT_VERSION,
  type UsageSummary,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { AsyncResult, Atom, AtomRegistry } from "effect/unstable/reactivity";
import { afterEach, describe, expect, it } from "vite-plus/test";

import type { EnvironmentPresentation } from "../connection/presentation.ts";
import { EnvironmentRpcUnavailableError } from "../rpc/client.ts";
import { createUsageLimitsRefresher, refreshUsage } from "./usage.ts";

const input = {
  sinceDay: UsageDay.make("2026-09-05"),
  untilDay: UsageDay.make("2026-09-05"),
  timeZone: "UTC",
};
const pricing = { status: "fresh" as const, source: "test", fetchedAt: null, knownModels: 1 };
const summary: UsageSummary = {
  ...input,
  contractVersion: USAGE_CONTRACT_VERSION,
  readAt: "2026-09-05T12:00:00Z",
  buckets: [],
  sources: [],
  pricing,
  scanDurationMs: 1,
};
const registries: AtomRegistry.AtomRegistry[] = [];
afterEach(() => {
  for (const registry of registries.splice(0)) registry.dispose();
});

function harness(ids = ["a"]) {
  const registry = AtomRegistry.make();
  registries.push(registry);
  const environments = ids.map((id) => {
    const environmentId = EnvironmentId.make(id);
    const rates = Promise.withResolvers<
      AsyncResult.Success<typeof pricing> | AsyncResult.Failure<never, unknown>
    >();
    const scan = Promise.withResolvers<UsageSummary>();
    const scanStarted = Promise.withResolvers<void>();
    const presentation = Atom.make({
      connection: { phase: "connected" },
    } as EnvironmentPresentation | null);
    const query = Atom.make(
      Effect.promise(() => {
        scanStarted.resolve();
        return scan.promise;
      }),
    );
    return { environmentId, rates, scan, scanStarted, presentation, query };
  });
  function get(environmentId: EnvironmentId) {
    const environment = environments.find((entry) => entry.environmentId === environmentId);
    if (!environment) throw new Error(`Unknown environment: ${environmentId}`);
    return environment;
  }
  const options = {
    registry,
    environmentIds: environments.map((entry) => entry.environmentId),
    input,
    server: {
      usageSummary: ({ environmentId }: { environmentId: EnvironmentId }) =>
        get(environmentId).query,
      refreshUsageRates: {
        label: "test:rates",
        run: (
          _registry: AtomRegistry.AtomRegistry,
          { environmentId }: { environmentId: EnvironmentId },
        ) => get(environmentId).rates.promise,
      },
    },
    presentations: {
      presentationAtom: (environmentId: EnvironmentId) => get(environmentId).presentation,
    },
  } satisfies Parameters<typeof refreshUsage>[0];
  return { registry, environments, refresh: () => refreshUsage(options) };
}

describe("manual usage refresh", () => {
  it.each(["success", "failure"])("waits for the rescan after a pricing %s", async (result) => {
    const {
      environments: [environment],
      refresh,
    } = harness();
    const entry = environment!;
    let finished = false;
    const refreshing = refresh().then(() => {
      finished = true;
    });
    expect(finished).toBe(false);
    entry.rates.resolve(
      result === "success"
        ? AsyncResult.success(pricing)
        : AsyncResult.fail(new Error("Pricing offline")),
    );
    await entry.scanStarted.promise;
    expect(finished).toBe(false);
    entry.scan.resolve(summary);
    await refreshing;
    expect(finished).toBe(true);
  });

  it("settles when an environment disconnects during the rescan", async () => {
    const {
      registry,
      environments: [environment],
      refresh,
    } = harness();
    const entry = environment!;
    const refreshing = refresh();
    entry.rates.resolve(AsyncResult.success(pricing));
    await entry.scanStarted.promise;
    registry.set(entry.presentation, null);
    await refreshing;
  });

  it("waits for healthy environments without waiting for a recovering environment", async () => {
    const { registry, environments, refresh } = harness(["healthy", "recovering"]);
    const [healthy, recovering] = environments;
    registry.set(recovering!.presentation, null);
    let finished = false;
    const refreshing = refresh().then(() => {
      finished = true;
    });
    for (const entry of environments) entry.rates.resolve(AsyncResult.success(pricing));
    await healthy!.scanStarted.promise;
    expect(finished).toBe(false);
    healthy!.scan.resolve(summary);
    await refreshing;
    expect(finished).toBe(true);
  });

  it("settles when connected state has no usable RPC session", async () => {
    const {
      environments: [environment],
      refresh,
    } = harness();
    const entry = environment!;
    const refreshing = refresh();
    entry.rates.resolve(
      AsyncResult.fail(
        new EnvironmentRpcUnavailableError({
          environmentId: entry.environmentId,
          message: "No session",
        }),
      ),
    );
    await refreshing;
  });

  it("replaces a scan that started before pricing was refreshed", async () => {
    const {
      registry,
      environments: [environment],
      refresh,
    } = harness();
    const entry = environment!;
    let reads = 0;
    const rescanned = Promise.withResolvers<void>();
    const query = Atom.make(
      Effect.promise(() => {
        reads += 1;
        if (reads > 1) {
          rescanned.resolve();
          return Promise.resolve(summary);
        }
        return new Promise<UsageSummary>(() => {});
      }),
    );
    entry.query = query;
    const unmount = registry.mount(query);
    expect(reads).toBe(1);
    const refreshing = refresh();
    entry.rates.resolve(AsyncResult.success(pricing));
    await rescanned.promise;
    await refreshing;
    expect(reads).toBe(2);
    unmount();
  });
});

describe("visible subscription limits", () => {
  const now = Date.parse("2026-09-13T12:00:00Z");
  const old = new Date(now - 2 * 60 * 60_000).toISOString();
  const presentation = (checkedAt: string) => ({
    entry: { target: { label: "Test" } },
    serverConfig: {
      usageLimitSources: [
        {
          id: UsageLimitSourceId.make("hub"),
          kind: "cliproxy" as const,
          label: "Hub",
          checkedAt,
          accounts: [],
        },
      ],
    },
  });

  it("refreshes stale environments independently and leaves fresh snapshots alone", async () => {
    const a = EnvironmentId.make("a"),
      b = EnvironmentId.make("b");
    const calls: EnvironmentId[] = [];
    const refresh = createUsageLimitsRefresher(async (id) => {
      calls.push(id);
    });
    await refresh(
      new Map([
        [a, presentation(old)],
        [b, presentation(new Date(now).toISOString())],
      ]),
      now,
    );
    expect(calls).toEqual([a]);
    await refresh(new Map([[b, presentation(old)]]), now);
    expect(calls).toEqual([a, b]);
  });

  it("coalesces reconnect and resume during an outstanding request, then retries after failure", async () => {
    const id = EnvironmentId.make("remote");
    const done = Promise.withResolvers<void>();
    let calls = 0;
    const refresh = createUsageLimitsRefresher(() => {
      calls++;
      return done.promise;
    });
    const views = new Map([[id, presentation(old)]]);
    const first = refresh(views, now);
    const reconnect = refresh(views, now + 1);
    await Promise.resolve();
    expect(calls).toBe(1);
    done.reject(new Error("disconnected"));
    await Promise.all([first, reconnect]);
    await refresh(views, now + 59_999);
    expect(calls).toBe(1);
    await refresh(views, now + 60_000);
    expect(calls).toBe(2);
  });
});
