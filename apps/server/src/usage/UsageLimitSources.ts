import {
  DEFAULT_PROVIDER_HEALTH_REFRESH_INTERVAL,
  UsageLimitSourceError,
  type UsageLimitSourceConsumeResetCreditInput,
  type ProviderConsumeResetCreditResult,
  type ProviderDriverKind,
  type ServerSettings,
  UsageLimitSourceConfig,
  settingsSecretLiteral,
  type UsageLimitSourceId,
  type UsageLimitSourceSnapshot,
} from "@t3tools/contracts";
import { resolveServerBackgroundActivitySettings } from "@t3tools/shared/backgroundActivitySettings";
import { antigravityWindowLimitsModel, hubRoutes } from "@t3tools/shared/usageLimits";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Fiber from "effect/Fiber";
import * as FiberHandle from "effect/FiberHandle";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import * as BackgroundPolicy from "../background/BackgroundPolicy.ts";
import * as SecretResolver from "@t3tools/secret-provider-core/server/SecretResolver";
import * as SecretResolverLayer from "../secrets/SecretResolver.ts";
import { resolveSettingsSecrets } from "../secrets/settingsSecretValue.ts";
import * as Settings from "../serverSettings.ts";
import { windowLimitsModel } from "../provider/claudeUsageLimits.ts";
import { makeCliproxyApi } from "./cliproxyApi.ts";

export class UsageLimitSources extends Context.Service<
  UsageLimitSources,
  {
    readonly current: Effect.Effect<ReadonlyArray<UsageLimitSourceSnapshot>>;
    readonly streamChanges: Stream.Stream<ReadonlyArray<UsageLimitSourceSnapshot>>;
    readonly refresh: Effect.Effect<void>;
    readonly consumeResetCredit: (
      input: UsageLimitSourceConsumeResetCreditInput,
    ) => Effect.Effect<ProviderConsumeResetCreditResult, UsageLimitSourceError>;
  }
>()("t3/usage/UsageLimitSources") {}

function sourceLabel(id: string, config: UsageLimitSourceConfig): string {
  if (config.label) return config.label;
  const fallback = id.trim() || "Usage limit source";
  try {
    return new URL(config.url).host || fallback;
  } catch {
    return fallback;
  }
}

/** How long after it was read a reading may stand in for a refused read of the same account. */
const READING_MAX_AGE_MS = 30_000;

/**
 * Shows `read`, except that an account whose read was refused keeps the reading `shown` has for it
 * while that reading is younger than READING_MAX_AGE_MS. Quota can change at any moment, so an
 * older one is replaced by the failure, which then carries when the account was last read.
 * `expiresAt` is when the first kept reading gets too old.
 */
function keepRecentReadings(
  shown: ReadonlyArray<UsageLimitSourceSnapshot>,
  read: ReadonlyArray<UsageLimitSourceSnapshot>,
  now: number,
): {
  readonly snapshots: ReadonlyArray<UsageLimitSourceSnapshot>;
  readonly expiresAt: number | null;
} {
  let expiresAt: number | null = null;
  const snapshots = read.map((source) => {
    // Auth file names can be reused, so only the same hub and login may lend a reading.
    const before = shown.find(
      (candidate) => candidate.id === source.id && candidate.endpoint === source.endpoint,
    );
    if (!before) return source;
    return {
      ...source,
      accounts: source.accounts.map((account) => {
        const failure = account.usageLimits.unavailable;
        if (failure?.reason !== "probeFailed") return account;
        // Without a listed login, a reused auth file name cannot be told apart from the old one.
        if (account.email === undefined && account.workspaceId === undefined) return account;
        const last = before.accounts.find(
          (candidate) =>
            candidate.id === account.id &&
            candidate.driver === account.driver &&
            // Fingerprinted Antigravity readings omit the listed identity, so they are never lent.
            candidate.usageLimits.credentialFingerprint === undefined &&
            candidate.email === account.email &&
            candidate.workspaceId === account.workspaceId,
        );
        if (!last) return account;
        const readAt = last.usageLimits.unavailable ? null : Date.parse(last.usageLimits.checkedAt);
        if (readAt !== null && now - readAt < READING_MAX_AGE_MS) {
          expiresAt = Math.min(expiresAt ?? Infinity, readAt + READING_MAX_AGE_MS);
          // Only the reading is lent; an unread model list stays unknown.
          return { ...last, models: account.models };
        }
        const lastReadAt =
          readAt === null ? last.usageLimits.unavailable?.lastReadAt : last.usageLimits.checkedAt;
        return lastReadAt === undefined
          ? account
          : {
              ...account,
              usageLimits: { ...account.usageLimits, unavailable: { ...failure, lastReadAt } },
            };
      }),
    };
  });
  return { snapshots, expiresAt };
}

function missingManagementKey(config: UsageLimitSourceConfig): string {
  return typeof config.managementKey === "string"
    ? "No management key configured."
    : "Could not read the management key from its secret provider.";
}

/** @public */
export const make = Effect.gen(function* () {
  const api = yield* makeCliproxyApi;
  const settingsService = yield* Settings.ServerSettingsService;
  const secrets = yield* SecretResolver.SecretResolver;
  const backgroundPolicy = yield* BackgroundPolicy.BackgroundPolicy;
  const readManagementKey = Effect.fn("UsageLimitSources.readManagementKey")(function* (
    config: UsageLimitSourceConfig,
  ) {
    const resolved = yield* resolveSettingsSecrets(secrets, UsageLimitSourceConfig, config);
    return settingsSecretLiteral(resolved.managementKey);
  });
  const stateRef = yield* Ref.make<ReadonlyArray<UsageLimitSourceSnapshot>>([]);
  // What the hubs last answered, before any refused account was given its recent reading back.
  const readRef = yield* Ref.make<ReadonlyArray<UsageLimitSourceSnapshot>>([]);
  const expiry = yield* FiberHandle.make();
  const changes = yield* Effect.acquireRelease(
    PubSub.unbounded<ReadonlyArray<UsageLimitSourceSnapshot>>(),
    PubSub.shutdown,
  );

  const readSource = Effect.fn("UsageLimitSources.readSource")(function* (
    id: UsageLimitSourceId,
    config: UsageLimitSourceConfig,
  ) {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const url = URL.parse(config.url);
    const base = {
      id,
      kind: config.kind,
      label: sourceLabel(id, config),
      ...(url && /^https?:$/.test(url.protocol)
        ? {
            origin: url.origin,
            endpoint: `${url.origin}${url.pathname.replace(/(\/management\.html)?\/*$/i, "")}`,
          }
        : {}),
      checkedAt,
    } as const;
    const managementKey = yield* readManagementKey(config);
    if (managementKey.length === 0) {
      return { ...base, accounts: [], error: missingManagementKey(config) };
    }
    const accounts = yield* api
      .readAccounts({ url: config.url, managementKey })
      .pipe(Effect.result);
    if (accounts._tag === "Failure") {
      yield* Effect.logDebug("usage limit source read failed", { id, cause: accounts.failure });
      return { ...base, accounts: [], error: accounts.failure.detail };
    }
    return { ...base, accounts: accounts.success };
  });

  const publish = Effect.fn("UsageLimitSources.publish")(function* (
    next: ReadonlyArray<UsageLimitSourceSnapshot>,
  ) {
    const changed = yield* Ref.modify(stateRef, (previous) =>
      Equal.equals(previous, next) ? [false, previous] : [true, next],
    );
    if (changed) yield* PubSub.publish(changes, next);
  });

  // Serialize refreshes so slow old-settings reads cannot resurrect removed sources after newer refreshes.
  const refreshLock = yield* Semaphore.make(1);

  // Held while the shown state changes and never across a hub request, so a refresh stuck on a
  // slow hub cannot keep a reading up past its age.
  const publishLock = yield* Semaphore.make(1);
  const changeShown = <A, E>(change: Effect.Effect<A, E>) =>
    change.pipe(publishLock.withPermits(1), Effect.uninterruptible);

  /**
   * Publishes the hubs' last answers with recent readings kept, and returns when the first kept
   * reading gets too old. Run it through `changeShown`.
   */
  const publishRead = Effect.gen(function* () {
    const kept = keepRecentReadings(
      yield* Ref.get(stateRef),
      yield* Ref.get(readRef),
      yield* Clock.currentTimeMillis,
    );
    yield* publish(kept.snapshots);
    return kept.expiresAt;
  });

  // No refresh may come for minutes, so a kept reading is taken down by its own timer.
  const expireKeptReadings = Effect.fn("UsageLimitSources.expireKeptReadings")(function* (
    expiresAt: number | null,
  ) {
    if (expiresAt === null) return yield* FiberHandle.clear(expiry);
    yield* FiberHandle.run(
      expiry,
      Effect.gen(function* () {
        let next: number | null = expiresAt;
        while (next !== null) {
          yield* Effect.sleep(Math.max(0, next - (yield* Clock.currentTimeMillis)));
          next = yield* changeShown(publishRead);
        }
      }),
    );
  });

  const readAll = Effect.gen(function* () {
    const settings = yield* settingsService.getSettings.pipe(
      Effect.orElseSucceed((): ServerSettings | null => null),
    );
    const entries = Object.entries(settings?.usageLimitSources ?? {}).filter(
      ([, config]) => config.enabled,
    );
    const snapshots = yield* Effect.forEach(
      entries,
      ([id, config]) => readSource(id as UsageLimitSourceId, config),
      { concurrency: 4 },
    );
    yield* changeShown(Effect.andThen(Ref.set(readRef, snapshots), publishRead)).pipe(
      Effect.flatMap(expireKeptReadings),
    );
  }).pipe(Effect.ignoreCause({ log: true }));

  // A caller may stop waiting, as the reset fallback does after five seconds. While queued it
  // just leaves the queue. Once its refresh has the lock, the refresh runs to its end in the
  // service's scope, so a refusal it met still reaches the shown state.
  const scope = yield* Effect.scope;
  const refresh = Effect.uninterruptibleMask((restore) =>
    restore(refreshLock.take(1)).pipe(
      Effect.andThen(
        readAll.pipe(
          Effect.interruptible,
          Effect.ensuring(refreshLock.release(1)),
          Effect.forkIn(scope),
        ),
      ),
      Effect.flatMap((fiber) => restore(Fiber.join(fiber))),
    ),
  );

  // Shares the refresh lock so a stale in-flight read cannot overwrite a redemption.
  const consumeResetCredit = Effect.fn("UsageLimitSources.consumeResetCredit")(function* (
    input: UsageLimitSourceConsumeResetCreditInput,
  ) {
    const settings = yield* settingsService.getSettings.pipe(
      Effect.mapError(() => new UsageLimitSourceError({ detail: "Could not read hub settings." })),
    );
    const config = settings.usageLimitSources[input.sourceId];
    if (!config?.enabled) {
      return yield* new UsageLimitSourceError({
        detail: "The usage limit source is missing or disabled.",
      });
    }
    const managementKey = yield* readManagementKey(config);
    if (managementKey.length === 0) {
      return yield* new UsageLimitSourceError({ detail: missingManagementKey(config) });
    }
    const result = yield* api.consume(
      { url: config.url, managementKey },
      input.accountId,
      input.creditId,
    );
    // A redemption changes the reading, so a refused re-read must not bring back the one before it.
    const snapshot = yield* readSource(input.sourceId, config);
    const replace = (sources: ReadonlyArray<UsageLimitSourceSnapshot>) =>
      sources.map((source) => (source.id === input.sourceId ? snapshot : source));
    yield* changeShown(
      Effect.gen(function* () {
        yield* Ref.update(readRef, replace);
        yield* publish(replace(yield* Ref.get(stateRef)));
      }),
    );
    return result;
  }, refreshLock.withPermits(1));

  yield* settingsService.streamChanges.pipe(
    Stream.map((settings) => settings.usageLimitSources),
    Stream.changes,
    Stream.runForEach(() => refresh),
    Effect.forkScoped,
  );

  const interval = settingsService.getSettings.pipe(
    Effect.map(
      (settings) => resolveServerBackgroundActivitySettings(settings).providerHealthRefreshInterval,
    ),
    Effect.orElseSucceed(() => DEFAULT_PROVIDER_HEALTH_REFRESH_INTERVAL),
  );
  yield* Effect.forever(
    interval.pipe(
      Effect.flatMap((wait) => Effect.sleep(Duration.isPositive(wait) ? wait : "60 seconds")),
      Effect.andThen(interval),
      Effect.flatMap((current) =>
        Duration.isPositive(current)
          ? backgroundPolicy.shouldRunScopeWork({ type: "provider-status" })
          : Effect.succeed(false),
      ),
      Effect.flatMap((shouldRun) => (shouldRun ? refresh : Effect.void)),
      Effect.ignoreCause({ log: true }),
    ),
  ).pipe(Effect.forkScoped);

  yield* refresh.pipe(Effect.forkScoped);

  return {
    current: Ref.get(stateRef),
    consumeResetCredit,
    refresh,
    get streamChanges() {
      return Stream.unwrap(
        Effect.gen(function* () {
          const subscription = yield* PubSub.subscribe(changes);
          const snapshot = yield* Ref.get(stateRef);
          return Stream.concat(Stream.make(snapshot), Stream.fromSubscription(subscription)).pipe(
            Stream.changes,
          );
        }),
      );
    },
  } satisfies UsageLimitSources["Service"];
});

export const layer = Layer.effect(UsageLimitSources, make).pipe(
  Layer.provide(SecretResolverLayer.layer),
);

/** A hub returns a usage limit only when every account it routes the model to is spent. */
export function hubPoolResetAt(
  sources: ReadonlyArray<UsageLimitSourceSnapshot>,
  driver: ProviderDriverKind,
  model: string | null,
  now: number,
): string | null {
  let earliest: number | null = null;
  const routes = hubRoutes(sources, driver, model);
  for (const account of sources.flatMap((source) => source.accounts).filter(routes)) {
    // An unread account may be the one still blocked, so its refill is unknown.
    if (account.usageLimits.unavailable) return null;
    const spent = account.usageLimits.windows.filter(
      (window) =>
        window.usedPercent >= 100 &&
        windowLimitsModel(window, model) &&
        (account.driver !== "antigravity" || antigravityWindowLimitsModel(window, model)),
    );
    if (spent.length === 0) return null;
    const resets = spent.map((window) => Date.parse(window.resetsAt ?? ""));
    if (resets.some((reset) => !Number.isFinite(reset))) return null;
    const recoversAt = Math.max(...resets);
    if (recoversAt <= now) return null;
    earliest = earliest === null ? recoversAt : Math.min(earliest, recoversAt);
  }
  return earliest === null ? null : DateTime.formatIso(DateTime.makeUnsafe(earliest));
}

export class UsageLimitResetFallback extends Context.Reference<{
  readonly resetAt: (
    driver: ProviderDriverKind,
    endpoint: string,
    model: string | null,
  ) => Effect.Effect<string | null>;
}>("t3/usage/UsageLimitResetFallback", {
  defaultValue: () => ({ resetAt: () => Effect.succeed(null) }),
}) {}

export const layerResetFallback = Layer.effect(
  UsageLimitResetFallback,
  Effect.gen(function* () {
    const sources = yield* UsageLimitSources;
    return {
      resetAt: Effect.fn("UsageLimitResetFallback.resetAt")(function* (driver, endpoint, model) {
        // Only the hub at the instance's endpoint serves its turns.
        const atEndpoint = Effect.map(sources.current, (current) =>
          current.filter((source) => source.origin === endpoint),
        );
        if ((yield* atEndpoint).length === 0) return null;
        // The pool usually ran out after the last poll, so the cached accounts still show room.
        yield* sources.refresh.pipe(Effect.timeout("5 seconds"), Effect.ignore);
        return hubPoolResetAt(
          yield* atEndpoint,
          driver,
          model,
          DateTime.toEpochMillis(yield* DateTime.now),
        );
      }),
    };
  }),
);
