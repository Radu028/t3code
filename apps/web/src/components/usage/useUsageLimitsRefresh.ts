import { useAtomValue } from "@effect/atom-react";
import { createUsageLimitsRefresher } from "@t3tools/client-runtime/state/usage";
import type { EnvironmentId } from "@t3tools/contracts";
import { USAGE_LIMITS_MAX_AGE_MS } from "@t3tools/shared/usageLimits";
import { useEffect, useEffectEvent, useMemo } from "react";

import { environmentPresentations } from "../../state/presentation";
import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";

export function useUsageLimitsRefresh(
  enabled: boolean,
  selectedEnvironmentIds: ReadonlySet<EnvironmentId> | null,
  onChecked: (now: number) => void,
) {
  const presentations = useAtomValue(environmentPresentations.presentationsAtom);
  const refreshProviders = useAtomCommand(serverEnvironment.refreshProviders, {
    reportFailure: false,
  });
  const refresh = useMemo(
    () =>
      createUsageLimitsRefresher((environmentId) => refreshProviders({ environmentId, input: {} })),
    [refreshProviders],
  );
  const update = useEffectEvent(() => {
    if (!enabled || document.visibilityState !== "visible") return;
    const selected = new Map(
      [...presentations].filter(
        ([id, presentation]) =>
          presentation.connection.phase === "connected" &&
          (selectedEnvironmentIds === null || selectedEnvironmentIds.has(id)),
      ),
    );
    onChecked(Date.now());
    void refresh(selected, Date.now());
  });
  useEffect(() => {
    update();
    // Recheck after config delivery, reconnect, or environment selection.
    // eslint-disable-next-line react/exhaustive-effect-dependencies
  }, [enabled, presentations, selectedEnvironmentIds]);
  useEffect(() => {
    if (!enabled) return;
    let timer: ReturnType<typeof setInterval> | undefined;
    const wake = () => {
      clearInterval(timer);
      update();
      if (document.visibilityState === "visible")
        timer = setInterval(update, USAGE_LIMITS_MAX_AGE_MS);
    };
    wake();
    document.addEventListener("visibilitychange", wake);
    window.addEventListener("focus", wake);
    window.addEventListener("online", wake);
    return () => {
      clearInterval(timer);
      document.removeEventListener("visibilitychange", wake);
      window.removeEventListener("focus", wake);
      window.removeEventListener("online", wake);
    };
  }, [enabled]);
}
