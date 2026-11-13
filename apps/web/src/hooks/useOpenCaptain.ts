import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_SERVER_SETTINGS,
  type EnvironmentId,
  type ModelSelection,
  type ScopedProjectRef,
  type ServerProvider,
  type ServerSettings,
  type ThreadId,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import { resolveNewThreadModelSelection } from "@t3tools/shared/usageLimits";
import { useRouter } from "@tanstack/react-router";

import { toastManager } from "../components/ui/toast";
import { useComposerDraftStore } from "../composerDraftStore";
import { randomUUID } from "../lib/utils";
import {
  resolveDefaultProviderModelSelection,
  resolveProjectDefaultProviderModelSelection,
} from "../providerInstances";
import { appAtomRegistry } from "../rpc/atomRegistry";
import { readProject, readThreadShells, useServerConfigs } from "../state/entities";
import { orchestrationEnvironment } from "../state/orchestration";
import { environmentServerConfigsAtom } from "../state/server";
import { useAtomCommand } from "../state/use-atom-command";
import { buildThreadRouteParams } from "../threadRoutes";
import { findLoadedCaptain } from "./useOpenCaptain.logic";

/** A new captain starts on the captain model, else the project's default, else the model the user last chose. */
function resolveCaptainModelSelection(input: {
  readonly projectRef: ScopedProjectRef;
  readonly captainModel: ModelSelection | null;
  readonly projectDefault: ModelSelection | null;
  readonly lastUsed: ModelSelection | null;
  readonly providers: ReadonlyArray<ServerProvider>;
  readonly settings: ServerSettings;
  readonly now: number;
}): ModelSelection | null {
  return resolveNewThreadModelSelection({
    defaultSelection:
      input.captainModel ??
      input.projectDefault ??
      // The last choice can name a provider this project's environment lacks.
      (input.lastUsed === null
        ? null
        : resolveDefaultProviderModelSelection(input.providers, input.lastUsed)) ??
      resolveProjectDefaultProviderModelSelection(
        input.providers,
        input.settings,
        input.projectRef.projectId,
      ),
    fallbackSelection: input.settings.fallbackModelSelection ?? null,
    providers: input.providers,
    settings: input.settings,
    projectId: input.projectRef.projectId,
    now: input.now,
  }).selection;
}

/** Hides the captain on environments whose server cannot serve it. */
export function useCaptainSupported(): (environmentId: EnvironmentId) => boolean {
  const configs = useServerConfigs();
  return (environmentId) =>
    configs.get(environmentId)?.environment.capabilities.projectCaptain === true;
}

/** Opens a project's captain, creating it on first use. */
export function useOpenCaptain(): (projectRef: ScopedProjectRef) => Promise<void> {
  const router = useRouter();
  const ensureCaptain = useAtomCommand(orchestrationEnvironment.ensureCaptain);

  return async (projectRef) => {
    const open = (threadId: ThreadId) =>
      router.navigate({
        to: "/$environmentId/$threadId",
        params: buildThreadRouteParams(scopeThreadRef(projectRef.environmentId, threadId)),
      });
    const loaded = findLoadedCaptain(readThreadShells(), projectRef);
    if (loaded !== null) {
      await open(loaded.id);
      return;
    }

    const config = appAtomRegistry.get(environmentServerConfigsAtom).get(projectRef.environmentId);
    const settings = config?.settings ?? DEFAULT_SERVER_SETTINGS;
    const project = readProject(projectRef);
    const projectSettings = resolveProjectSettings(
      settings,
      projectRef.projectId,
      project,
    ).settings;
    const { stickyActiveProvider, stickyModelSelectionByProvider } =
      useComposerDraftStore.getState();
    const modelSelection = resolveCaptainModelSelection({
      projectRef,
      captainModel: settings.captainModelSelection,
      projectDefault: projectSettings.defaultModelSelection ?? null,
      lastUsed:
        stickyActiveProvider === null
          ? null
          : (stickyModelSelectionByProvider[stickyActiveProvider] ?? null),
      providers: config?.providers ?? [],
      settings,
      now: Date.now(),
    });
    if (modelSelection === null) {
      toastManager.add({
        type: "error",
        title: "Could not open the captain",
        description: "No provider is ready for this project.",
      });
      return;
    }

    const result = await ensureCaptain({
      environmentId: projectRef.environmentId,
      input: {
        commandId: CommandId.make(randomUUID()),
        creationSource: "web",
        projectId: projectRef.projectId,
        modelSelection,
        runtimeMode: projectSettings.defaultRuntimeMode,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      },
    });
    if (result._tag === "Failure") {
      if (isAtomCommandInterrupted(result)) return;
      const error = squashAtomCommandFailure(result);
      toastManager.add({
        type: "error",
        title: "Could not open the captain",
        description: error instanceof Error ? error.message : undefined,
      });
      return;
    }
    await open(result.value.threadId);
  };
}
