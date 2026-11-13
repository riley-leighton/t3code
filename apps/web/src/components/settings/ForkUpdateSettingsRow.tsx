import { useAtomValue } from "@effect/atom-react";
import { useState } from "react";

import { primaryForkUpdateAtom, useStartForkUpdate } from "../../hooks/useStartForkUpdate";
import { Button } from "../ui/button";
import { SettingsRow } from "./settingsLayout";
import { searchableSetting } from "./settingsSearch";

/** Whether the fork's source checkout has upstream's newest stable release, and a way to merge it. */
export function ForkUpdateSettingsRow() {
  const forkUpdate = useAtomValue(primaryForkUpdateAtom);
  const forkUpdateAction = useStartForkUpdate();
  const [pending, setPending] = useState(false);

  if (forkUpdate === undefined) return null;

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
    void forkUpdateAction.start(forkUpdate).finally(() => setPending(false));
  };

  return (
    <SettingsRow
      {...searchableSetting("fork-update")}
      description={`Update to ${forkUpdate.latestStableTag}. Opens a thread in a worktree of ${forkUpdate.sourceDir} on update/${forkUpdate.latestStableTag} that merges it and rebuilds this app. You send the prompt and install the new build yourself.`}
      control={
        <Button
          size="sm"
          variant="outline"
          disabled={pending || !forkUpdateAction.ready}
          onClick={start}
        >
          Update
        </Button>
      }
    />
  );
}
