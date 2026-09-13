import type { EnvironmentId, UsageSummaryInput } from "@t3tools/contracts";
import {
  USAGE_LIMITS_MAX_AGE_MS,
  usageLimitsAreStale,
  type LimitPresentations,
} from "@t3tools/shared/usageLimits";
import * as Schema from "effect/Schema";
import type { AtomRegistry } from "effect/unstable/reactivity";

import { EnvironmentRpcUnavailableError } from "../rpc/client.ts";
import type { createEnvironmentPresentationAtoms } from "./presentation.ts";
import { executeAtomQuery, runAtomCommand, squashAtomCommandFailure } from "./runtime.ts";
import type { createServerEnvironmentAtoms } from "./server.ts";

const isEnvironmentRpcUnavailable = Schema.is(EnvironmentRpcUnavailableError);

/** Only visible consumers call this; bound retries and coalesce overlapping wakeups per environment. */
export function createUsageLimitsRefresher(refresh: (id: EnvironmentId) => Promise<unknown>) {
  const attempted = new Map<EnvironmentId, number>();
  const pending = new Map<EnvironmentId, Promise<unknown>>();
  return async (presentations: LimitPresentations, now: number) => {
    await Promise.allSettled(
      [...presentations].map(([id, presentation]) => {
        const config = presentation.serverConfig;
        const timestamps = [
          ...(config?.usageLimitSources ?? []).map((source) => source.checkedAt),
          ...(config?.providers ?? []).flatMap((provider) =>
            provider.enabled &&
            provider.usageLimits?.unavailable?.reason !== "unsupported" &&
            provider.usageLimits
              ? [provider.usageLimits.checkedAt]
              : [],
          ),
        ];
        if (!timestamps.some((checkedAt) => usageLimitsAreStale(checkedAt, now))) return;
        const running = pending.get(id);
        if (running) return running;
        if (now - (attempted.get(id) ?? -Infinity) < USAGE_LIMITS_MAX_AGE_MS) return;
        attempted.set(id, now);
        const request = Promise.resolve()
          .then(() => refresh(id))
          .finally(() => pending.delete(id));
        pending.set(id, request);
        return request;
      }),
    );
  };
}

/** Refresh pricing, then await each selected environment's rescan while it remains connected. */
export async function refreshUsage({
  registry,
  server,
  presentations,
  environmentIds,
  input,
}: {
  registry: AtomRegistry.AtomRegistry;
  server: Pick<
    ReturnType<typeof createServerEnvironmentAtoms>,
    "usageSummary" | "refreshUsageRates"
  >;
  presentations: Pick<ReturnType<typeof createEnvironmentPresentationAtoms>, "presentationAtom">;
  environmentIds: readonly EnvironmentId[];
  input: UsageSummaryInput;
}): Promise<void> {
  await Promise.all(
    environmentIds.map(async (environmentId) => {
      const query = server.usageSummary({ environmentId, input });
      const presentation = presentations.presentationAtom(environmentId);
      const controller = new AbortController();
      const abortWhenDisconnected = () => {
        if (registry.get(presentation)?.connection.phase !== "connected") controller.abort();
      };
      const unsubscribe = registry.subscribe(presentation, abortWhenDisconnected);
      abortWhenDisconnected();
      try {
        const ratesResult = await runAtomCommand(
          registry,
          server.refreshUsageRates,
          { environmentId, input: {} },
          { reportFailure: false },
        );
        const sessionUnavailable =
          ratesResult._tag === "Failure" &&
          isEnvironmentRpcUnavailable(squashAtomCommandFailure(ratesResult));
        // Invalidate even on failure so reconnects cannot reuse the old summary.
        registry.refresh(query);
        if (sessionUnavailable || controller.signal.aborted) return;
        await executeAtomQuery(registry, query, {
          reportFailure: false,
          signal: controller.signal,
        });
      } finally {
        unsubscribe();
      }
    }),
  );
}
