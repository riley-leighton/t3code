import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { isCurrentCaptainThread, type ScopedProjectRef } from "@t3tools/contracts";

type CaptainCandidate = Pick<
  EnvironmentThreadShell,
  "environmentId" | "projectId" | "captain" | "archivedAt" | "deletedAt" | "createdAt"
> & { readonly id: string };

/** The project's captain among the threads this client has loaded; the server decides when none is. */
export function findLoadedCaptain<T extends CaptainCandidate>(
  threads: ReadonlyArray<T>,
  projectRef: ScopedProjectRef,
): T | null {
  return (
    threads
      .filter(
        (thread) =>
          thread.environmentId === projectRef.environmentId &&
          thread.projectId === projectRef.projectId &&
          isCurrentCaptainThread(thread),
      )
      // ISO timestamps order as text.
      .toSorted((left, right) => right.createdAt.localeCompare(left.createdAt))[0] ?? null
  );
}
