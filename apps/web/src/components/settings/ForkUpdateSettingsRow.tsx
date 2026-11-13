import { useState } from "react";

import { useOpenLocalWorkspace } from "../../hooks/useOpenLocalWorkspace";
import { randomUUID } from "../../lib/utils";
import { usePrimaryEnvironment } from "../../state/environments";
import { Button } from "../ui/button";
import { stackedThreadToast, toastManager } from "../ui/toast";
import { SettingsRow } from "./settingsLayout";
import { searchableSetting } from "./settingsSearch";

/** The checkout this build came from. The fork has no update feed, so it updates from source. */
const FORK_SOURCE_DIR = import.meta.env.T3CODE_FORK_SOURCE_DIR;

const FORK_UPDATE_PROMPT = `Update this fork of T3 Code from upstream and rebuild the desktop app.

Follow "Update from upstream" in docs/operations/fork.md, in this checkout (${FORK_SOURCE_DIR}).

- If the work tree has uncommitted changes, stop and ask me what to do with them.
- If upstream has nothing new, say so and stop.
- Do not push.
- When the DMG is built, open it and tell me to quit the app and replace it in Applications.`;

/** Opens a thread in the fork's checkout that merges upstream T3 Code and rebuilds the app. */
export function ForkUpdateSettingsRow() {
  const primaryEnvironment = usePrimaryEnvironment();
  const openLocalWorkspace = useOpenLocalWorkspace();
  const [pending, setPending] = useState(false);

  const os = primaryEnvironment?.serverConfig?.environment.platform.os;
  if (FORK_SOURCE_DIR === "" || os === undefined || os === "unknown") return null;

  const start = () => {
    setPending(true);
    void openLocalWorkspace
      .open(
        {
          version: 1,
          requestId: randomUUID(),
          type: "open-workspace",
          workspaceRoot: FORK_SOURCE_DIR,
          platform: os === "windows" ? "win32" : os,
        },
        { prompt: FORK_UPDATE_PROMPT },
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
      description={`Opens a thread in ${FORK_SOURCE_DIR} that merges the latest T3 Code and rebuilds this app. You send the prompt and install the new build yourself.`}
      control={
        <Button
          size="sm"
          variant="outline"
          disabled={pending || !openLocalWorkspace.ready}
          onClick={start}
        >
          Update from upstream
        </Button>
      }
    />
  );
}
