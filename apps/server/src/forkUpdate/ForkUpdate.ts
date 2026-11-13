import { GitCommandError, type ServerForkUpdate } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../config.ts";
import { forkParked } from "../serverActivation.ts";
import { GIT_ENV, parseLsRemote, T3CODE_REPOSITORY_URL } from "../triage/TriageService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";

/** Whether upstream T3 Code's newest stable release is in the fork's source checkout. */
export class ForkUpdate extends Context.Service<
  ForkUpdate,
  {
    /** Emits the current status, then each change. */
    readonly streamChanges: Stream.Stream<ServerForkUpdate | undefined>;
  }
>()("t3/forkUpdate/ForkUpdate") {}

const CHECK_INTERVAL = Duration.hours(6);

const STABLE_TAG = /^refs\/tags\/v(\d+)\.(\d+)\.(\d+)$/;

const isNewer = (version: ReadonlyArray<number>, than: ReadonlyArray<number>) => {
  const index = version.findIndex((part, i) => part !== than[i]);
  return index !== -1 && version[index]! > than[index]!;
};

/** The highest `vX.Y.Z` tag `git ls-remote --tags` listed, peeled to its commit. */
const newestStableTag = (lsRemoteOutput: string) => {
  const refs = parseLsRemote(lsRemoteOutput);
  let newest: { readonly ref: string; readonly version: ReadonlyArray<number> } | undefined;
  for (const ref of refs.keys()) {
    const match = STABLE_TAG.exec(ref);
    if (match === null) continue;
    const version = match.slice(1).map(Number);
    if (newest === undefined || isNewer(version, newest.version)) newest = { ref, version };
  }
  if (newest === undefined) return undefined;
  return {
    tag: newest.ref.slice("refs/tags/".length),
    // Annotated tags list the tag object first and the commit as `^{}`.
    commit: refs.get(`${newest.ref}^{}`) ?? refs.get(newest.ref)!,
  };
};

/** No status when the checkout is missing, is not a work tree, or upstream has no stable tag. */
export const check = Effect.fn("ForkUpdate.check")(function* (input: {
  readonly sourceDir: string;
  readonly upstreamUrl: string;
}) {
  const git = yield* GitVcsDriver.GitVcsDriver;
  const fs = yield* FileSystem.FileSystem;
  if (input.sourceDir === "" || !(yield* fs.exists(input.sourceDir))) return undefined;
  const workTree = yield* git.execute({
    operation: "ForkUpdate.isWorkTree",
    cwd: input.sourceDir,
    args: ["rev-parse", "--is-inside-work-tree"],
    allowNonZeroExit: true,
    timeoutMs: 10_000,
  });
  if (workTree.stdout.trim() !== "true") return undefined;
  const listed = yield* git.execute({
    operation: "ForkUpdate.listTags",
    cwd: input.sourceDir,
    args: ["ls-remote", "--tags", input.upstreamUrl],
    env: GIT_ENV,
    timeoutMs: 30_000,
  });
  const newest = newestStableTag(listed.stdout);
  if (newest === undefined) return undefined;
  const present = yield* git.execute({
    operation: "ForkUpdate.hasTagCommit",
    cwd: input.sourceDir,
    args: ["cat-file", "-e", `${newest.commit}^{commit}`],
    allowNonZeroExit: true,
    timeoutMs: 10_000,
  });
  if (present.exitCode !== 0) {
    // By URL and with no destination, so the fetch adds no remote, tag or FETCH_HEAD.
    yield* git.execute({
      operation: "ForkUpdate.fetchTag",
      cwd: input.sourceDir,
      args: [
        "fetch",
        "--quiet",
        "--no-tags",
        "--no-write-fetch-head",
        input.upstreamUrl,
        `refs/tags/${newest.tag}`,
      ],
      env: GIT_ENV,
      timeoutMs: 300_000,
    });
  }
  // HEAD, because the DMG is built from whatever the checkout has checked out.
  const ancestry = yield* git.execute({
    operation: "ForkUpdate.isMerged",
    cwd: input.sourceDir,
    args: ["merge-base", "--is-ancestor", newest.commit, "HEAD"],
    allowNonZeroExit: true,
    timeoutMs: 30_000,
  });
  if (ancestry.exitCode !== 0 && ancestry.exitCode !== 1) {
    return yield* new GitCommandError({
      operation: "ForkUpdate.isMerged",
      command: "git merge-base --is-ancestor",
      cwd: input.sourceDir,
      exitCode: ancestry.exitCode,
      detail: ancestry.stderr.trim(),
    });
  }
  return {
    sourceDir: input.sourceDir,
    latestStableTag: newest.tag,
    merged: ancestry.exitCode === 0,
  } satisfies ServerForkUpdate;
});

const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const git = yield* GitVcsDriver.GitVcsDriver;
  const fs = yield* FileSystem.FileSystem;
  const status = yield* Ref.make<ServerForkUpdate | undefined>(undefined);
  const changes = yield* PubSub.sliding<ServerForkUpdate | undefined>(1);

  const refresh = check({
    sourceDir: config.forkSourceDir ?? "",
    upstreamUrl: T3CODE_REPOSITORY_URL,
  }).pipe(
    Effect.provideService(GitVcsDriver.GitVcsDriver, git),
    Effect.provideService(FileSystem.FileSystem, fs),
    // Offline or a moved checkout: nothing the user can act on, so hide the status.
    Effect.catch((cause) =>
      Effect.logWarning("Could not check for a fork update", { cause }).pipe(Effect.as(undefined)),
    ),
    Effect.flatMap((next) =>
      Ref.modify(status, (previous) =>
        Equal.equals(previous, next) ? [false, previous] : [true, next],
      ).pipe(Effect.flatMap((changed) => (changed ? PubSub.publish(changes, next) : Effect.void))),
    ),
  );
  yield* forkParked(refresh.pipe(Effect.repeat(Schedule.spaced(CHECK_INTERVAL))));

  return {
    get streamChanges() {
      return Stream.unwrap(
        Effect.gen(function* () {
          const subscription = yield* PubSub.subscribe(changes);
          const snapshot = yield* Ref.get(status);
          return Stream.concat(Stream.make(snapshot), Stream.fromSubscription(subscription)).pipe(
            Stream.changes,
          );
        }),
      );
    },
  } satisfies ForkUpdate["Service"];
});

export const layer = Layer.effect(ForkUpdate, make);
