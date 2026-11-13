import { useAtomValue } from "@effect/atom-react";
import { DownloadIcon } from "lucide-react";
import { useState } from "react";

import { primaryForkUpdateAtom, useStartForkUpdate } from "../../hooks/useStartForkUpdate";
import { cn } from "../../lib/utils";
import { SidebarMenuItem, useSidebar } from "../ui/sidebar";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";

/** Fork-only: shows while the source checkout lacks upstream's newest stable release. */
export function SidebarForkUpdatePill() {
  const forkUpdate = useAtomValue(primaryForkUpdateAtom);
  const forkUpdateAction = useStartForkUpdate();
  const { isMobile, setOpenMobile } = useSidebar();
  const [pending, setPending] = useState(false);

  if (forkUpdate?.merged !== false) return null;

  const disabled = pending || !forkUpdateAction.ready;
  const start = () => {
    if (disabled) return;
    if (isMobile) setOpenMobile(false);
    setPending(true);
    void forkUpdateAction.start(forkUpdate).finally(() => setPending(false));
  };

  return (
    // flex-1 takes the row's free space ahead of the official pill's auto margin, so the two sit together.
    <SidebarMenuItem className="flex min-w-0 flex-1 justify-end">
      <Tooltip>
        <TooltipTrigger
          render={
            <button
              type="button"
              aria-label={`Update to ${forkUpdate.latestStableTag}`}
              aria-disabled={disabled || undefined}
              className={cn(
                "inline-flex h-8 min-w-0 items-center gap-1.5 rounded-full bg-sidebar-control-surface px-2.5 text-xs font-medium text-sidebar-foreground outline-hidden ring-ring transition-colors focus-visible:ring-2 focus-visible:ring-inset",
                disabled
                  ? "cursor-not-allowed opacity-60"
                  : "cursor-pointer hover:bg-sidebar-row-hover",
              )}
              onClick={start}
            >
              <DownloadIcon className="size-3.5 shrink-0" />
              <span className="truncate">{forkUpdate.latestStableTag}</span>
            </button>
          }
        />
        <TooltipPopup side="top">
          Update to T3 Code {forkUpdate.latestStableTag} in a new worktree thread
        </TooltipPopup>
      </Tooltip>
    </SidebarMenuItem>
  );
}
