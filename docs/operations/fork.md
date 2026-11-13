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

The server checks every 6 hours whether upstream's newest stable release (`vX.Y.Z`) is in this
checkout. **Settings → About** says "Includes T3 Code vX.Y.Z" when it is, and "Update to vX.Y.Z" with
an **Update** button when it is not. The button opens a thread in this checkout with these steps as
its prompt. To do it by hand:

1. Commit or stash your work. Stop if `git status` is not clean.
2. `git fetch upstream`, then `git merge upstream/main` into the branch you build from. Nothing new
   means nothing to do.
3. Resolve conflicts so the captain, the table above and the update button still work.
4. `vp install`, then `vp run check:changed`. Fix what the merge broke.
5. Build the DMG as above and open it.
6. Quit T3 Code (Captain), drag the new app over the old one in Applications and start it again.

Push to `origin` only when you mean to publish the merge.
