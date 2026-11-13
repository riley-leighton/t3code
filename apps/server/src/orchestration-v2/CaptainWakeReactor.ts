import {
  CommandId,
  MessageId,
  isCurrentCaptainThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type RunId,
  type RuntimeRequestId,
  type ThreadId,
} from "@t3tools/contracts";
import { T3_CODE_CAPTAIN_NO_UPDATE } from "@t3tools/provider-core/server/orchestrationInstructions";
import { delegatedTaskProgress } from "@t3tools/provider-core/server/subagentProjection";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FiberMap from "effect/FiberMap";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import { forkParked } from "../serverActivation.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";

/** Updates that arrive this close together reach the captain as one turn. */
export const CAPTAIN_WAKE_DEBOUNCE = Duration.seconds(3);
/** Wakes a captain may take in a row before they pause until the user writes to it. */
export const MAX_UNATTENDED_CAPTAIN_WAKES = 20;
const MAX_HELD_NOTICES = 100;
/** How many of T3 Code's own follow-up runs to look behind for who asked for the work. */
const MAX_FOLLOW_UP_RUNS = 20;
const WAKE_MESSAGE_PREFIX = "message:captain-wake:";

const ACTIVE_RUN_STATUSES = ["preparing", "starting", "running", "waiting"] as const;
const ENDED_RUN_STATUSES: ReadonlyArray<OrchestrationV2Run["status"]> = [
  "completed",
  "failed",
  "interrupted",
  "cancelled",
];

interface Notice {
  readonly key: string;
  readonly threadId: ThreadId;
  readonly title: string;
  readonly kind: "completed" | "failed" | "question";
  /** Set on a result, to tell it from the one a wait already handed the captain. */
  readonly runId?: RunId;
  readonly requestId?: RuntimeRequestId;
}

const noticeLine = (notice: Notice) => {
  const thread = `"${notice.title}" (threadId ${notice.threadId})`;
  if (notice.kind === "completed") return `- ${thread} finished.`;
  if (notice.kind === "failed") return `- ${thread} failed.`;
  return `- ${thread} asked a question and is waiting for the answer. Read it with t3_pending_request action read, that threadId and requestId ${notice.requestId}.`;
};

const wakeText = (notices: ReadonlyArray<Notice>, last: boolean) =>
  [
    "Automatic wake-up: no one wrote to you. Work you asked for has updates.",
    ...notices.map(noticeLine),
    `Read what you need with t3_thread_read. If nothing needs doing or saying, reply exactly ${T3_CODE_CAPTAIN_NO_UPDATE}.`,
    ...(last
      ? [
          "This is your last automatic wake-up until the user writes to you again. Tell them so in this reply.",
        ]
      : []),
  ].join("\n");

const wakeSummary = (notices: ReadonlyArray<Notice>) => {
  const [only] = notices;
  if (notices.length !== 1 || only === undefined) return `${notices.length} threads have updates`;
  if (only.kind === "question") return `"${only.title}" has a question`;
  return `"${only.title}" ${only.kind === "failed" ? "failed" : "finished"}`;
};

/**
 * Starts a captain turn when a thread finishes, fails or asks a question in work the captain
 * asked for: its runs begin from a message the captain sent, or follow up on one. Updates wait
 * while the captain is mid-turn and go out together when it ends. Held updates and the wake
 * count live in memory, so a server restart drops the ones not yet sent.
 */
export class CaptainWakeReactor extends Context.Service<
  CaptainWakeReactor,
  {
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
    readonly handle: (event: OrchestrationV2DomainEvent) => Effect.Effect<void>;
  }
>()("t3/orchestration-v2/CaptainWakeReactor") {}

const make = Effect.gen(function* () {
  const engine = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const settings = yield* ServerSettings.ServerSettingsService;
  const flushes = yield* FiberMap.make<ThreadId>();

  const held = new Map<ThreadId, Map<string, Notice>>();
  /** The last result each captain was told about, per thread, so a later follow-up cannot repeat it. */
  const reported = new Map<string, RunId>();
  const unattendedWakes = new Map<ThreadId, number>();

  /** The captain whose message started this run, or the run it follows up on. */
  const captainOf = Effect.fnUntraced(function* (
    threadId: ThreadId,
    runs: ReadonlyArray<OrchestrationV2Run>,
    runId: RunId,
  ) {
    const newestFirst = runs.toSorted((a, b) => b.ordinal - a.ordinal);
    const start = newestFirst.findIndex((run) => run.id === runId);
    if (start === -1) return undefined;
    const candidates = newestFirst.slice(start, start + MAX_FOLLOW_UP_RUNS);
    const messages = yield* projections.getMessagesById(
      threadId,
      candidates.map((run) => run.userMessageId),
    );
    for (const run of candidates) {
      const message = messages.get(run.userMessageId);
      if (message === undefined) return undefined;
      if (message.senderThreadId !== undefined) {
        if (message.senderThreadId === threadId) return undefined;
        // A deleted sender has no thread to wake.
        const sender = yield* projections
          .getThread(message.senderThreadId)
          .pipe(Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.undefined }));
        return sender !== undefined && isCurrentCaptainThread(sender) ? sender.id : undefined;
      }
      // T3 Code's own follow-ups continue the work an earlier message asked for.
      const followsUp =
        message.notification !== undefined ||
        run.restartContinuationOfRunId !== undefined ||
        run.usageLimitContinuationOfRunId !== undefined;
      if (!followsUp) return undefined;
    }
    return undefined;
  });

  const hold = Effect.fnUntraced(function* (captainId: ThreadId, notice: Notice) {
    const notices = held.get(captainId) ?? new Map<string, Notice>();
    if (!notices.has(notice.key) && notices.size >= MAX_HELD_NOTICES) {
      const [oldest] = notices.keys();
      if (oldest !== undefined) notices.delete(oldest);
    }
    notices.set(notice.key, notice);
    held.set(captainId, notices);
    yield* scheduleFlush(captainId);
  });

  const flush = Effect.fnUntraced(function* (captainId: ThreadId) {
    yield* Effect.sleep(CAPTAIN_WAKE_DEBOUNCE);
    const notices = held.get(captainId);
    if (notices === undefined || notices.size === 0) return;
    if (!(yield* settings.getSettings).captainWakes) {
      held.delete(captainId);
      return;
    }
    const captain = yield* projections.getThreadRecords(captainId, ["runs"], {
      runStatuses: ACTIVE_RUN_STATUSES,
    });
    if (!isCurrentCaptainThread(captain.thread)) {
      held.delete(captainId);
      return;
    }
    // Mid-turn the captain may be reading these threads itself; its run ending flushes again.
    if (captain.runs.length > 0) return;
    const wakes = unattendedWakes.get(captainId) ?? 0;
    if (wakes >= MAX_UNATTENDED_CAPTAIN_WAKES) return;

    const due: Array<Notice> = [];
    for (const notice of notices.values()) {
      if (notice.kind === "question") {
        const request = yield* projections.getRuntimeRequest(notice.threadId, notice.requestId!);
        if (request?.status === "pending") due.push(notice);
        continue;
      }
      const reportKey = `${captainId}:${notice.threadId}`;
      const seen =
        reported.get(reportKey) === notice.runId ||
        (yield* threads.waitDeliveredRun(captainId, notice.threadId)) === notice.runId;
      if (!seen) due.push(notice);
    }
    held.delete(captainId);
    const [first] = due;
    if (first === undefined) return;

    const last = wakes + 1 >= MAX_UNATTENDED_CAPTAIN_WAKES;
    yield* engine.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`server:captain-wake:${captainId}:${first.key}`),
      threadId: captainId,
      messageId: MessageId.make(`${WAKE_MESSAGE_PREFIX}${captainId}:${first.key}`),
      text: wakeText(due, last),
      notification: {
        source: { kind: "system" },
        outcome: due.some((notice) => notice.kind === "failed") ? "failed" : "updated",
        summary: wakeSummary(due),
      },
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "agent",
      creationSource: "server",
    });
    unattendedWakes.set(captainId, wakes + 1);
    for (const notice of due) {
      if (notice.runId !== undefined) {
        reported.set(`${captainId}:${notice.threadId}`, notice.runId);
      }
    }
  });

  const scheduleFlush = (captainId: ThreadId) =>
    FiberMap.run(flushes, captainId, { onlyIfMissing: true })(
      flush(captainId).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("waking a captain failed", { threadId: captainId, cause }),
        ),
      ),
    );

  // Runs for every domain event; most events return before any work is needed.
  const handle: CaptainWakeReactor["Service"]["handle"] = Effect.fnUntraced(
    function* (event) {
      if (event.type === "run.created") {
        // Only the user writing lifts the pause: other agents and T3 Code's own turns do not.
        if (
          !unattendedWakes.has(event.threadId) ||
          event.payload.userMessageId.startsWith(WAKE_MESSAGE_PREFIX)
        ) {
          return;
        }
        const message = (yield* projections.getMessagesById(event.threadId, [
          event.payload.userMessageId,
        ])).get(event.payload.userMessageId);
        if (message?.createdBy === "user") unattendedWakes.delete(event.threadId);
        return;
      }

      if (event.type === "runtime-request.updated") {
        if (
          event.payload.kind !== "user_input" ||
          event.payload.status !== "pending" ||
          event.runId === undefined
        ) {
          return;
        }
        const asking = yield* projections.getThreadRecords(event.threadId, ["runs"]);
        const captainId = yield* captainOf(event.threadId, asking.runs, event.runId);
        if (captainId === undefined) return;
        yield* hold(captainId, {
          key: `${event.threadId}:question:${event.payload.id}`,
          threadId: event.threadId,
          title: asking.thread.title,
          kind: "question",
          requestId: event.payload.id,
        });
        return;
      }

      if (event.type !== "run.updated" || !ENDED_RUN_STATUSES.includes(event.payload.status)) {
        return;
      }
      // The captain's own turn ended: send what was held while it ran.
      if (held.has(event.threadId)) yield* scheduleFlush(event.threadId);

      const ended = yield* projections.getThreadRecords(event.threadId, ["runs"]);
      if (
        ended.thread.lineage.relationshipToParent === "subagent" ||
        ended.thread.lineage.sideChat === true ||
        (yield* captainOf(event.threadId, ended.runs, event.payload.id)) === undefined
      ) {
        return;
      }
      // Report once the thread owes no more work, as t3_thread_wait decides it.
      const work = yield* projections.getThreadRecords(
        event.threadId,
        ["runs", "messages", "subagents", "providerThreads"],
        { messageRoles: ["user"], monitorNotificationsOnly: true },
      );
      const { state, resultRun } = delegatedTaskProgress(work);
      if (
        state !== "result_available" ||
        resultRun === undefined ||
        (resultRun.status !== "completed" && resultRun.status !== "failed") ||
        (yield* engine.delegatedTaskResultPending(event.threadId))
      ) {
        return;
      }
      const captainId = yield* captainOf(event.threadId, work.runs, resultRun.id);
      if (captainId === undefined) return;
      yield* hold(captainId, {
        key: `${event.threadId}:result:${resultRun.id}`,
        threadId: event.threadId,
        title: work.thread.title,
        kind: resultRun.status,
        runId: resultRun.id,
      });
    },
    (effect, event) =>
      effect.pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("handling an event for a captain wake failed", {
            threadId: event.threadId,
            cause,
          }),
        ),
      ),
  );

  const start: CaptainWakeReactor["Service"]["start"] = () =>
    forkParked(
      Stream.runForEach(engine.streamDomainEvents, handle).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("captain wake event stream failed", { cause }),
        ),
      ),
    );

  return { start, handle } satisfies CaptainWakeReactor["Service"];
});

export const layer = Layer.effect(CaptainWakeReactor, make);
