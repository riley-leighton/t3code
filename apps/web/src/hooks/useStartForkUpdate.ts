import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_SERVER_SETTINGS,
  type ScopedProjectRef,
  type ServerForkUpdate,
  type ThreadId,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import { useRouter } from "@tanstack/react-router";
import { Atom } from "effect/reactivity";

import { stackedThreadToast, toastManager } from "../components/ui/toast";
import { useComposerDraftStore } from "../composerDraftStore";
import { newThreadId, randomUUID } from "../lib/utils";
import { appAtomRegistry } from "../rpc/atomRegistry";
import { readProject, readThreadShells } from "../state/entities";
import { usePrimaryEnvironment } from "../state/environments";
import { orchestrationEnvironment } from "../state/orchestration";
import { environmentServerConfigsAtom, primaryServerConfigAtom } from "../state/server";
import { useAtomCommand } from "../state/use-atom-command";
import { vcsEnvironment } from "../state/vcs";
import { buildThreadRouteParams } from "../threadRoutes";
import { resolveCaptainModelSelection } from "./useOpenCaptain";
import { useOpenLocalWorkspace } from "./useOpenLocalWorkspace";

export const primaryForkUpdateAtom = Atom.make(
  (get): ServerForkUpdate | undefined => get(primaryServerConfigAtom)?.forkUpdate,
).pipe(Atom.withLabel("web-primary-fork-update"));

const forkUpdatePrompt = ({
  latestStableTag,
  sourceDir,
}: ServerForkUpdate) => `Update this fork of T3 Code to upstream's ${latestStableTag} release and rebuild the desktop app.

You are in a worktree on branch update/${latestStableTag}, cut from main. The source checkout the app is built from is ${sourceDir}.

Follow "Update from upstream" in docs/operations/fork.md. Merge the tag ${latestStableTag}, not upstream/main. Do not push.`;

/**
 * Starts merging the fork update status's release: opens the project's `update/<tag>`
 * thread, or launches one in a new worktree with the prompt in its composer, unsent.
 * The Settings row and the sidebar pill both call this.
 */
export function useStartForkUpdate() {
  const router = useRouter();
  const primaryEnvironment = usePrimaryEnvironment();
  const openLocalWorkspace = useOpenLocalWorkspace();
  const launchThread = useAtomCommand(orchestrationEnvironment.launchThread, {
    reportFailure: false,
  });
  const readRefs = useAtomCommand(vcsEnvironment.readRefs, { reportFailure: false });

  const openUpdateThread = async (projectRef: ScopedProjectRef, forkUpdate: ServerForkUpdate) => {
    const branch = `update/${forkUpdate.latestStableTag}`;
    const navigateTo = (threadId: ThreadId) =>
      router
        .navigate({
          to: "/$environmentId/$threadId",
          params: buildThreadRouteParams(scopeThreadRef(projectRef.environmentId, threadId)),
        })
        .then(() => ({ threadId }));

    const existing = readThreadShells().find(
      (thread) =>
        thread.environmentId === projectRef.environmentId &&
        thread.projectId === projectRef.projectId &&
        thread.branch === branch &&
        thread.deletedAt === null,
    );
    if (existing !== undefined) return navigateTo(existing.id);

    // An archived or paged-out thread can own the branch; a launch would then fail to create the
    // worktree and leave a thread on the source checkout itself.
    const refs = await readRefs({
      environmentId: projectRef.environmentId,
      input: { cwd: forkUpdate.sourceDir, query: branch, refKind: "local" },
    });
    if (refs._tag === "Failure") {
      const error = squashAtomCommandFailure(refs);
      throw error instanceof Error ? error : new Error("T3 Code could not read the branches.");
    }
    if (refs.value.refs.some((ref) => ref.name === branch)) {
      throw new Error(
        `Branch ${branch} already exists in ${forkUpdate.sourceDir}. If its thread is archived, unarchive it. Otherwise delete the branch to start again.`,
      );
    }

    const config = appAtomRegistry.get(environmentServerConfigsAtom).get(projectRef.environmentId);
    const settings = config?.settings ?? DEFAULT_SERVER_SETTINGS;
    const projectSettings = resolveProjectSettings(
      settings,
      projectRef.projectId,
      readProject(projectRef),
    ).settings;
    const { stickyActiveProvider, stickyModelSelectionByProvider, setPrompt } =
      useComposerDraftStore.getState();
    // The composer can change this before the first send.
    const modelSelection = resolveCaptainModelSelection({
      projectRef,
      captainModel: null,
      projectDefault: projectSettings.defaultModelSelection ?? null,
      lastUsed:
        stickyActiveProvider === null
          ? null
          : (stickyModelSelectionByProvider[stickyActiveProvider] ?? null),
      providers: config?.providers ?? [],
      settings,
      now: Date.now(),
    });
    if (modelSelection === null) throw new Error("No provider is ready for this project.");

    const result = await launchThread({
      environmentId: projectRef.environmentId,
      input: {
        commandId: CommandId.make(randomUUID()),
        creationSource: "web",
        threadId: newThreadId(),
        projectId: projectRef.projectId,
        title: `Update to ${forkUpdate.latestStableTag}`,
        modelSelection,
        runtimeMode: projectSettings.defaultRuntimeMode,
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        workspaceStrategy: { type: "worktree", baseRef: "main", branch },
      },
    });
    if (result._tag === "Failure") {
      const error = squashAtomCommandFailure(result);
      throw error instanceof Error ? error : new Error("T3 Code could not launch the thread.");
    }
    const threadId = result.value.threadId;
    setPrompt(scopeThreadRef(projectRef.environmentId, threadId), forkUpdatePrompt(forkUpdate));
    return navigateTo(threadId);
  };

  const os = primaryEnvironment?.serverConfig?.environment.platform.os;
  const start = async (forkUpdate: ServerForkUpdate) => {
    if (os === undefined || os === "unknown") return;
    const response = await openLocalWorkspace.open(
      {
        version: 1,
        requestId: randomUUID(),
        type: "open-workspace",
        workspaceRoot: forkUpdate.sourceDir,
        platform: os === "windows" ? "win32" : os,
      },
      { openThread: (projectRef) => openUpdateThread(projectRef, forkUpdate) },
    );
    if (response.ok) return;
    toastManager.add(
      stackedThreadToast({
        type: "error",
        title: "Could not start the update",
        description: response.message,
      }),
    );
  };

  return { ready: openLocalWorkspace.ready && os !== undefined && os !== "unknown", start };
}
