import { describe, expect, it } from "@effect/vitest";
import { UsageLimitSourceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import * as BackgroundPolicy from "../background/BackgroundPolicy.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import * as UsageLimitSources from "./UsageLimitSources.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

function fixture() {
  let reads = 0;
  let failing = false;
  const http = HttpClient.make((request) =>
    Effect.sync(() => {
      if (request.url.endsWith("/auth-files")) {
        reads++;
        return HttpClientResponse.fromWeb(
          request,
          Response.json(
            { files: [{ id: "test", auth_index: "test", provider: "claude" }] },
            { status: failing ? 503 : 200 },
          ),
        );
      }
      return HttpClientResponse.fromWeb(
        request,
        Response.json({
          status_code: 200,
          body: encodeJson({
            five_hour: { utilization: reads === 1 ? 44 : 51, resets_at: null },
          }),
        }),
      );
    }),
  );
  const layer = UsageLimitSources.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(HttpClient.HttpClient, http),
        Layer.mock(BackgroundPolicy.BackgroundPolicy)({
          shouldRunScopeWork: () => Effect.succeed(false),
        }),
        ServerSettingsService.layerTest({
          usageLimitSources: {
            [UsageLimitSourceId.make("hub")]: {
              kind: "cliproxy",
              url: "http://hub.test",
              managementKey: "test",
              enabled: true,
            },
          },
        }),
      ),
    ),
  );
  return {
    layer,
    reads: () => reads,
    fail: () => {
      failing = true;
    },
  };
}

const populated = (sources: UsageLimitSources.UsageLimitSources["Service"]) =>
  sources.streamChanges.pipe(
    Stream.filter((snapshots) => snapshots.length > 0),
    Stream.take(1),
    Stream.runCollect,
  );

describe("UsageLimitSources freshness", () => {
  it.effect(
    "stays idle without demand, then revalidates a stale subscription and coalesces reconnecting clients",
    () => {
      const test = fixture();
      return Effect.gen(function* () {
        const sources = yield* UsageLimitSources.UsageLimitSources;
        yield* populated(sources);
        expect(test.reads()).toBe(1);
        yield* TestClock.adjust("2 hours");
        expect(test.reads()).toBe(1);
        expect((yield* sources.current)[0]?.accounts[0]?.usageLimits.windows[0]?.usedPercent).toBe(
          44,
        );
        const refreshed = sources.streamChanges.pipe(
          Stream.filter(
            (snapshots) => snapshots[0]?.accounts[0]?.usageLimits.windows[0]?.usedPercent === 51,
          ),
          Stream.take(1),
          Stream.runCollect,
        );
        const clients = yield* Effect.all([refreshed, refreshed], { concurrency: "unbounded" });
        expect(clients).toHaveLength(2);
        expect(test.reads()).toBe(2);
        yield* populated(sources);
        expect(test.reads()).toBe(2);
      }).pipe(Effect.provide(test.layer));
    },
  );

  it.effect(
    "publishes failures to an existing subscriber and bounds failed reconnect retries",
    () => {
      const test = fixture();
      return Effect.gen(function* () {
        const sources = yield* UsageLimitSources.UsageLimitSources;
        yield* populated(sources);
        yield* TestClock.adjust("2 hours");
        test.fail();
        const failure = yield* sources.streamChanges.pipe(
          Stream.filter((snapshots) => snapshots[0]?.error !== undefined),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkChild,
        );
        const snapshots = yield* Fiber.join(failure);
        expect(snapshots[0]?.[0]?.error).toBe("The hub could not list accounts.");
        expect(snapshots[0]?.[0]?.accounts).toEqual([]);
        expect(test.reads()).toBe(2);
        yield* populated(sources);
        expect(test.reads()).toBe(2);
      }).pipe(Effect.provide(test.layer));
    },
  );
});
