import { useAtomValue } from "@effect/atom-react";
import type { ServerForkUpdate } from "@t3tools/contracts";
import { Atom } from "effect/reactivity";
import { useState } from "react";

import { useOpenLocalWorkspace } from "../../hooks/useOpenLocalWorkspace";
import { randomUUID } from "../../lib/utils";
import { usePrimaryEnvironment } from "../../state/environments";
import { primaryServerConfigAtom } from "../../state/server";
import { Button } from "../ui/button";
import { stackedThreadToast, toastManager } from "../ui/toast";
import { SettingsRow } from "./settingsLayout";
import { searchableSetting } from "./settingsSearch";

const primaryForkUpdateAtom = Atom.make(
  (get): ServerForkUpdate | undefined => get(primaryServerConfigAtom)?.forkUpdate,
).pipe(Atom.withLabel("web-primary-fork-update"));

const forkUpdatePrompt = (
  sourceDir: string,
) => `Update this fork of T3 Code from upstream and rebuild the desktop app.

Follow "Update from upstream" in docs/operations/fork.md, in this checkout (${sourceDir}).

- If the work tree has uncommitted changes, stop and ask me what to do with them.
- If upstream has nothing new, say so and stop.
- Do not push.
- When the DMG is built, open it and tell me to quit the app and replace it in Applications.`;

/** Whether the fork's source checkout has upstream's newest stable release, and a way to merge it. */
export function ForkUpdateSettingsRow() {
  const forkUpdate = useAtomValue(primaryForkUpdateAtom);
  const primaryEnvironment = usePrimaryEnvironment();
  const openLocalWorkspace = useOpenLocalWorkspace();
  const [pending, setPending] = useState(false);

  const os = primaryEnvironment?.serverConfig?.environment.platform.os;
  if (forkUpdate === undefined || os === undefined || os === "unknown") return null;

  if (forkUpdate.merged) {
    return (
      <SettingsRow
        {...searchableSetting("fork-update")}
        description={`Includes T3 Code ${forkUpdate.latestStableTag}.`}
      />
    );
  }

  const start = () => {
    setPending(true);
    void openLocalWorkspace
      .open(
        {
          version: 1,
          requestId: randomUUID(),
          type: "open-workspace",
          workspaceRoot: forkUpdate.sourceDir,
          platform: os === "windows" ? "win32" : os,
        },
        { prompt: forkUpdatePrompt(forkUpdate.sourceDir) },
      )
      .then((response) => {
        if (response.ok) return;
        toastManager.add(
          stackedThreadToast({
            type: "error",
            title: "Could not start the update",
            description: response.message,
          }),
        );
      })
      .finally(() => setPending(false));
  };

  return (
    <SettingsRow
      {...searchableSetting("fork-update")}
      description={`Update to ${forkUpdate.latestStableTag}. Opens a thread in ${forkUpdate.sourceDir} that merges it and rebuilds this app. You send the prompt and install the new build yourself.`}
      control={
        <Button
          size="sm"
          variant="outline"
          disabled={pending || !openLocalWorkspace.ready}
          onClick={start}
        >
          Update
        </Button>
      }
    />
  );
}
