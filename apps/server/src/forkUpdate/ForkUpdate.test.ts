import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import type * as Scope from "effect/Scope";

import * as ServerConfig from "../config.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as ForkUpdate from "./ForkUpdate.ts";

const TestLayer = GitVcsDriver.layer.pipe(
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-fork-update-" })),
  Layer.provideMerge(VcsProcess.layer),
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);

const git = (cwd: string, args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const driver = yield* GitVcsDriver.GitVcsDriver;
    const result = yield* driver.execute({
      operation: "ForkUpdate.test.git",
      cwd,
      args: ["-c", "user.name=Test", "-c", "user.email=test@example.com", ...args],
      timeoutMs: 10_000,
    });
    return result.stdout.trim();
  });

/**
 * A bare "upstream" with one commit, a source checkout cloned from it, and a
 * second clone that makes upstream's later commits and tags. The checkout never
 * fetches them, so the check has to.
 */
const makeRepos = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* fs.realPath(yield* fs.makeTempDirectoryScoped({ prefix: "t3-fork-update-" }));
  const upstreamUrl = path.join(root, "upstream.git");
  const sourceDir = path.join(root, "checkout");
  const releases = path.join(root, "releases");
  yield* fs.makeDirectory(upstreamUrl);
  yield* git(upstreamUrl, ["init", "--bare", "--quiet"]);
  yield* fs.makeDirectory(releases);
  yield* git(releases, ["init", "--quiet", "--initial-branch=main"]);
  yield* git(releases, ["commit", "--allow-empty", "--quiet", "-m", "base"]);
  yield* git(releases, ["push", "--quiet", upstreamUrl, "main"]);
  yield* git(root, ["clone", "--quiet", upstreamUrl, sourceDir]);

  const commit = (message: string) =>
    git(releases, ["commit", "--allow-empty", "--quiet", "-m", message]).pipe(
      Effect.andThen(git(releases, ["push", "--quiet", upstreamUrl, "main"])),
      Effect.andThen(git(releases, ["rev-parse", "HEAD"])),
    );
  const tag = (name: string, options?: { readonly annotated?: boolean }) =>
    git(
      releases,
      options?.annotated === true ? ["tag", "-a", name, "-m", name] : ["tag", name],
    ).pipe(Effect.andThen(git(releases, ["push", "--quiet", upstreamUrl, `refs/tags/${name}`])));

  return {
    upstreamUrl,
    sourceDir,
    commit,
    tag,
    check: ForkUpdate.check({ sourceDir, upstreamUrl }),
  };
});

const live = <A, E>(
  name: string,
  body: Effect.Effect<A, E, Layer.Success<typeof TestLayer> | Scope.Scope>,
) => it.live(name, () => body.pipe(Effect.scoped, Effect.provide(TestLayer)));

live(
  "reports no status when upstream has no stable tags",
  Effect.gen(function* () {
    const repos = yield* makeRepos;
    yield* repos.tag("v0.2.0-nightly.20261101");
    assert.isUndefined(yield* repos.check);
  }),
);

live(
  "reports the newest stable tag as merged when the checkout's history has it",
  Effect.gen(function* () {
    const repos = yield* makeRepos;
    yield* repos.tag("v0.1.9");
    assert.deepEqual(yield* repos.check, {
      sourceDir: repos.sourceDir,
      latestStableTag: "v0.1.9",
      merged: true,
    });
  }),
);

live(
  "reports a newer stable tag the checkout lacks as not merged",
  Effect.gen(function* () {
    const repos = yield* makeRepos;
    yield* repos.tag("v0.1.9");
    yield* repos.commit("release");
    yield* repos.tag("v0.1.10");
    assert.deepEqual(yield* repos.check, {
      sourceDir: repos.sourceDir,
      latestStableTag: "v0.1.10",
      merged: false,
    });
  }),
);

live(
  "ignores newer nightly and preview tags",
  Effect.gen(function* () {
    const repos = yield* makeRepos;
    yield* repos.tag("v0.1.9");
    yield* repos.commit("nightly");
    yield* repos.tag("v0.1.10-nightly.20261101");
    yield* repos.tag("v0.2.0-preview.1");
    assert.deepEqual(yield* repos.check, {
      sourceDir: repos.sourceDir,
      latestStableTag: "v0.1.9",
      merged: true,
    });
  }),
);

live(
  "resolves annotated tags to the commit they tag",
  Effect.gen(function* () {
    const repos = yield* makeRepos;
    yield* repos.tag("v0.1.9", { annotated: true });
    assert.deepEqual(yield* repos.check, {
      sourceDir: repos.sourceDir,
      latestStableTag: "v0.1.9",
      merged: true,
    });
    yield* repos.commit("release");
    yield* repos.tag("v0.1.10", { annotated: true });
    assert.deepEqual(yield* repos.check, {
      sourceDir: repos.sourceDir,
      latestStableTag: "v0.1.10",
      merged: false,
    });
  }),
);

live(
  "reports no status when the source checkout is missing",
  Effect.gen(function* () {
    const repos = yield* makeRepos;
    yield* repos.tag("v0.1.9");
    const fs = yield* FileSystem.FileSystem;
    yield* fs.remove(repos.sourceDir, { recursive: true });
    assert.isUndefined(yield* repos.check);
  }),
);
