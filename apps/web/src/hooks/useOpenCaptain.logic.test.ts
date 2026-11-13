import { scopeProjectRef } from "@t3tools/client-runtime/environment";
import { EnvironmentId, ProjectId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { findLoadedCaptain } from "./useOpenCaptain.logic";

const environmentId = EnvironmentId.make("environment-local");
const projectId = ProjectId.make("project-1");

const thread = (
  id: string,
  overrides: Partial<Parameters<typeof findLoadedCaptain>[0][number]> = {},
) => ({
  id,
  environmentId,
  projectId,
  captain: true,
  archivedAt: null,
  deletedAt: null,
  createdAt: "2026-11-01T00:00:00.000Z",
  ...overrides,
});

describe("findLoadedCaptain", () => {
  it("picks the project's newest captain that is not archived or deleted", () => {
    const captain = findLoadedCaptain(
      [
        thread("ordinary", { captain: false, createdAt: "2026-11-09T00:00:00.000Z" }),
        thread("retired", {
          archivedAt: "2026-11-05T00:00:00.000Z",
          createdAt: "2026-11-04T00:00:00.000Z",
        }),
        thread("older"),
        thread("current", { createdAt: "2026-11-03T00:00:00.000Z" }),
        thread("other-project", {
          projectId: ProjectId.make("project-2"),
          createdAt: "2026-11-08T00:00:00.000Z",
        }),
        thread("other-environment", {
          environmentId: EnvironmentId.make("environment-remote"),
          createdAt: "2026-11-08T00:00:00.000Z",
        }),
      ],
      scopeProjectRef(environmentId, projectId),
    );
    expect(captain?.id).toBe("current");
  });

  it("finds none when the project's only captain is archived", () => {
    expect(
      findLoadedCaptain(
        [thread("retired", { archivedAt: "2026-11-05T00:00:00.000Z" })],
        scopeProjectRef(environmentId, projectId),
      ),
    ).toBeNull();
  });
});
