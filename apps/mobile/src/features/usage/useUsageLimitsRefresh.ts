import { useAtomValue } from "@effect/atom-react";
import { createUsageLimitsRefresher } from "@t3tools/client-runtime/state/usage";
import type { EnvironmentId } from "@t3tools/contracts";
import { USAGE_LIMITS_MAX_AGE_MS } from "@t3tools/shared/usageLimits";
import { useEffect, useEffectEvent, useMemo } from "react";
import { AppState } from "react-native";
import { useIsFocused } from "@react-navigation/native";

import { environmentPresentations } from "../../state/presentation";
import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";

export function useUsageLimitsRefresh(
  enabled: boolean,
  selectedEnvironmentIds: ReadonlySet<EnvironmentId> | null,
  onChecked: (now: number) => void,
) {
  const focused = useIsFocused();
  const active = enabled && focused;
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
    if (!active || AppState.currentState !== "active") return;
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
  }, [active, presentations, selectedEnvironmentIds]);
  useEffect(() => {
    if (!active) return;
    let timer: ReturnType<typeof setInterval> | undefined;
    const wake = () => {
      clearInterval(timer);
      update();
      if (AppState.currentState === "active") timer = setInterval(update, USAGE_LIMITS_MAX_AGE_MS);
    };
    wake();
    const subscription = AppState.addEventListener("change", wake);
    return () => {
      clearInterval(timer);
      subscription.remove();
    };
  }, [active]);
}
