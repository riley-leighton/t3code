import {
  CommandId,
  MessageId,
  isCurrentCaptainThread,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type RunId,
  type RuntimeRequestId,
  type ThreadId,
} from "@t3tools/contracts";
import { T3_CODE_CAPTAIN_NO_UPDATE } from "@t3tools/provider-core/server/orchestrationInstructions";
import { delegatedTaskProgress } from "@t3tools/provider-core/server/subagentProjection";
import { backgroundWorkHoldsCompletion } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
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
const SETTLED_RUN_STATUSES: ReadonlyArray<OrchestrationV2Run["status"]> = [
  ...ENDED_RUN_STATUSES,
  "rolled_back",
];
/** What can end a thread's remaining work once its last run has ended. */
const SETTLING_EVENT_TYPES: ReadonlyArray<OrchestrationV2DomainEvent["type"]> = [
  "thread.pull-request-link-updated",
  "thread.pull-request-synced",
  "provider-thread.updated",
  "subagent.updated",
  "run.background-work-cancelled",
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
 * Starts a captain turn when work the captain asked for finishes, fails or asks a question.
 * Finished means no work owed, pull request watches included. Updates wait out the captain's
 * turn. Pending state lives in memory, so a server restart drops updates not yet sent.
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

  /** Captain work whose run ended while it still owed work, checked again as that work ends. */
  const unsettled = new Set<ThreadId>();

  const currentCaptain = Effect.fnUntraced(function* (threadId: ThreadId) {
    // A deleted thread has no turn to wake.
    const thread = yield* projections
      .getThread(threadId)
      .pipe(Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.undefined }));
    return thread !== undefined && isCurrentCaptainThread(thread) ? thread.id : undefined;
  });

  /**
   * The captain whose message started this run, or the run it follows up on. T3 Code's own
   * runs follow up: a delegated result, a pull request or monitor wake, a restart. A message from
   * the user, a scheduled task or another agent hands the work to them.
   */
  const captainOf = Effect.fnUntraced(function* (
    thread: Pick<OrchestrationV2AppThread, "id" | "launchedByThreadId">,
    runs: ReadonlyArray<OrchestrationV2Run>,
    runId: RunId,
  ) {
    const newestFirst = runs.toSorted((a, b) => b.ordinal - a.ordinal);
    const start = newestFirst.findIndex((run) => run.id === runId);
    if (start === -1) return undefined;
    const candidates = newestFirst.slice(start, start + MAX_FOLLOW_UP_RUNS);
    const messages = yield* projections.getMessagesById(
      thread.id,
      candidates.map((run) => run.userMessageId),
    );
    for (const run of candidates) {
      const message = messages.get(run.userMessageId);
      if (message?.senderThreadId !== undefined && message.senderThreadId !== thread.id) {
        return yield* currentCaptain(message.senderThreadId);
      }
      const followsUp =
        run.restartContinuationOfRunId !== undefined ||
        run.usageLimitContinuationOfRunId !== undefined ||
        (message !== undefined &&
          message.createdBy !== "user" &&
          (message.notification !== undefined ||
            message.delegatedCompletion !== undefined ||
            message.senderThreadId === thread.id));
      if (!followsUp) return undefined;
    }
    // Only follow-ups as far back as it looked: the work is still what its launcher asked for.
    return thread.launchedByThreadId === undefined
      ? undefined
      : yield* currentCaptain(thread.launchedByThreadId);
  });

  /**
   * Holds a notice once the thread owes no more work: what t3_thread_wait waits for, and also
   * no T3 Code wake-up still running and no pull request watch still open.
   */
  const reportIfSettled = Effect.fnUntraced(function* (threadId: ThreadId) {
    const work = yield* projections.getThreadRecords(
      threadId,
      ["runs", "messages", "subagents", "providerThreads"],
      { messageRoles: ["user"], monitorNotificationsOnly: true },
    );
    if (
      work.thread.lineage.relationshipToParent === "subagent" ||
      work.thread.lineage.sideChat === true
    ) {
      return;
    }
    const { state, resultRun } = delegatedTaskProgress(work);
    if (resultRun === undefined) return;
    const captainId = yield* captainOf(work.thread, work.runs, resultRun.id);
    if (captainId === undefined) {
      unsettled.delete(threadId);
      return;
    }
    const settled =
      state === "result_available" &&
      !work.runs.some(
        (run) =>
          !SETTLED_RUN_STATUSES.includes(run.status) &&
          !(run.status === "queued" && run.queueHeld === true),
      ) &&
      !backgroundWorkHoldsCompletion(
        (yield* projections.getThreadShell(threadId))?.pendingBackgroundTasks ?? [],
      ) &&
      !(yield* engine.delegatedTaskResultPending(threadId));
    if (!settled) {
      unsettled.add(threadId);
      return;
    }
    unsettled.delete(threadId);
    if (resultRun.status !== "completed" && resultRun.status !== "failed") return;
    yield* hold(captainId, {
      key: `${threadId}:result:${resultRun.id}`,
      threadId,
      title: work.thread.title,
      kind: resultRun.status,
      runId: resultRun.id,
    });
  });

  const hold = Effect.fnUntraced(function* (captainId: ThreadId, notice: Notice) {
    const notices = held.get(captainId) ?? new Map<string, Notice>();
    // A thread's latest result replaces the ones before it that the captain was not told yet.
    if (notice.runId !== undefined) {
      for (const [key, other] of notices) {
        if (other.threadId === notice.threadId && other.runId !== undefined) notices.delete(key);
      }
    }
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
        const captainId = yield* captainOf(asking.thread, asking.runs, event.runId);
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

      if (event.type === "run.updated" && ENDED_RUN_STATUSES.includes(event.payload.status)) {
        // The captain's own turn ended: send what was held while it ran.
        if (held.has(event.threadId)) yield* scheduleFlush(event.threadId);
        // Most runs are no captain's work, and this costs less than the full check.
        const ended = yield* projections.getThreadRecords(event.threadId, ["runs"]);
        if ((yield* captainOf(ended.thread, ended.runs, event.payload.id)) === undefined) {
          unsettled.delete(event.threadId);
          return;
        }
        yield* reportIfSettled(event.threadId);
        return;
      }
      // A watch, background task or subagent can end with no run left to end after it.
      if (unsettled.has(event.threadId) && SETTLING_EVENT_TYPES.includes(event.type)) {
        yield* reportIfSettled(event.threadId);
      }
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
