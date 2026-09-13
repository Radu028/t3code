import { act } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { EnvironmentId, UsageLimitSourceId } from "@t3tools/contracts";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

const state = vi.hoisted(() => ({
  focused: true,
  appState: "background",
  listeners: new Set<() => void>(),
  refresh: vi.fn(async () => undefined),
}));
vi.mock("react-native", () => ({
  AppState: {
    get currentState() {
      return state.appState;
    },
    addEventListener: (_: string, listener: () => void) => {
      state.listeners.add(listener);
      return { remove: () => state.listeners.delete(listener) };
    },
  },
}));
vi.mock("@react-navigation/native", () => ({ useIsFocused: () => state.focused }));
vi.mock("@effect/atom-react", () => ({ useAtomValue: () => presentations }));
vi.mock("../../state/presentation", () => ({
  environmentPresentations: { presentationsAtom: null },
}));
vi.mock("../../state/server", () => ({ serverEnvironment: { refreshProviders: null } }));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => state.refresh }));

import { useUsageLimitsRefresh } from "./useUsageLimitsRefresh";

const presentations = new Map([
  [
    EnvironmentId.make("remote"),
    {
      connection: { phase: "connected" },
      entry: { target: { label: "K12" } },
      serverConfig: {
        usageLimitSources: [
          {
            id: UsageLimitSourceId.make("hub"),
            kind: "cliproxy",
            label: "Hub",
            checkedAt: "2026-09-13T10:00:00Z",
            accounts: [],
          },
        ],
      },
    },
  ],
]);
function Screen({ enabled = true }) {
  useUsageLimitsRefresh(enabled, null, () => {});
  return null;
}
let renderer: ReactTestRenderer;
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-09-13T12:00:00Z"));
  state.focused = true;
  state.appState = "background";
  state.refresh.mockClear();
});
afterEach(async () => {
  await act(() => renderer?.unmount());
  vi.useRealTimers();
  vi.unstubAllGlobals();
});
it("refreshes on foreground resume, stops when leaving the screen, and recovers on return", async () => {
  await act(() => {
    renderer = create(<Screen />);
  });
  expect(vi.getTimerCount()).toBe(0);
  expect(state.refresh).not.toHaveBeenCalled();
  await act(async () => {
    state.appState = "active";
    for (const listener of state.listeners) listener();
  });
  expect(state.refresh).toHaveBeenCalledTimes(1);
  expect(state.refresh).toHaveBeenCalledWith({ environmentId: "remote", input: {} });
  await act(() => {
    state.focused = false;
    renderer.update(<Screen />);
  });
  expect(vi.getTimerCount()).toBe(0);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(2 * 60 * 60_000);
  });
  expect(state.refresh).toHaveBeenCalledTimes(1);
  await act(() => {
    state.focused = true;
    renderer.update(<Screen />);
  });
  expect(state.refresh).toHaveBeenCalledTimes(2);
});
it("does not refresh the cost/tokens tab", async () => {
  state.appState = "active";
  await act(() => {
    renderer = create(<Screen enabled={false} />);
  });
  expect(state.refresh).not.toHaveBeenCalled();
  expect(vi.getTimerCount()).toBe(0);
});
