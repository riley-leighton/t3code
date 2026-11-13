# Building and updating the Captain fork

This checkout is a fork of [T3 Code](https://github.com/pingdotgg/t3code) that adds
[project captains](../user/thread-sidebar.md#ask-the-projects-captain). Its desktop app installs
beside the official one and never reads the official app's data.

## What the fork keeps separate

|                 | Official app         | This fork                    |
| --------------- | -------------------- | ---------------------------- |
| App name        | T3 Code (Beta)       | T3 Code (Captain)            |
| App id          | `com.t3tools.t3code` | `com.t3tools.t3code.captain` |
| T3 home         | `~/.t3`              | `~/.t3-captain`              |
| Browser profile | `t3code-v2`          | `t3code-captain`             |

A merge from upstream must keep the right-hand column. Sharing the T3 home would let an older
fork build open a database the official app has already migrated.

Both apps still answer `t3code://` links, share the boot service's launchd label and install the
same `t3` command, so macOS and your shell pick one of them for those.

## Build the app

The scripts need the Node version in `.node-version`. If `node --version` differs, put Vite+'s copy
first for the shell you build in:

```sh
export PATH="$HOME/.local/share/vite-plus/js_runtime/node/$(cat .node-version)/bin:$PATH"
```

Then build:

```sh
vp run dist:desktop:dmg
```

The DMG lands in `release/`. It is unsigned: it opens normally on the Mac that built it, and a copy
sent to another Mac needs **Open Anyway** under **System Settings → Privacy & Security**. The app
has no update feed and never updates itself.

## Update from upstream

The fork follows upstream's stable releases (`vX.Y.Z`) only, never `upstream/main`, nightly or
preview builds. The server checks every 6 hours whether the newest one is in this checkout.
**Settings → About** says "Includes T3 Code vX.Y.Z" when it is, and "Update to vX.Y.Z" with an
**Update** button when it is not, and a vX.Y.Z update pill appears in the sidebar. The
button and the pill open a thread in a new worktree on branch `update/vX.Y.Z`, cut from `main`,
with a prompt naming the release. Pick the model and send it. Pressing either again opens the same
thread.

The agent, or you by hand, then:

1. Adds the `upstream` remote (`https://github.com/pingdotgg/t3code`) if it is missing, and fetches
   the tag: `git fetch upstream tag vX.Y.Z`.
2. In the update worktree, runs `git merge vX.Y.Z` and resolves conflicts so the captain, the table
   above and this update flow still work.
3. Runs `vp install`, then `vp run check:changed`, and fixes what the merge broke.
4. In the source checkout, stops and asks if `git status` is not clean, the branch is not `main`, or
   `main` has commits the update branch lacks, so it cannot fast-forward
   (`git merge-base --is-ancestor main update/vX.Y.Z` fails). It never stashes, switches branches or
   rebases there. Otherwise it runs `git merge --ff-only update/vX.Y.Z` on `main`.
5. Builds the DMG in the source checkout, not the worktree, as above, and opens it. The build
   records the checkout's path for the next update, so it must be one that stays.
6. Removes the update worktree: `git worktree remove <path>`.
7. Tells you to quit T3 Code (Captain), drag the new app over the old one in Applications and start
   it again.

Never push. Publishing the merge to `origin` is your decision.
