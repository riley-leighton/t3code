import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  type RunId,
  RuntimeRequestId,
  ThreadId,
  type ThreadPullRequestLink,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as CaptainWakeReactor from "./CaptainWakeReactor.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed for captain wakes"),
} as ProviderAdapter.ProviderAdapterV2["Service"];

/** Results a wait already handed a captain, keyed like the service keeps them. */
const waited = new Map<string, RunId>();

const layerTest = (settings: Parameters<typeof ServerSettings.layerTest>[0] = {}) => {
  const layerDatabase = SqlitePersistence.layerMemory;
  return CaptainWakeReactor.layer.pipe(
    Layer.provide(ServerSettings.layerTest(settings)),
    Layer.provide(
      Layer.mock(ThreadManagement.ThreadManagementService)({
        waitDeliveredRun: (waiterThreadId, threadId) =>
          Effect.sync(() => waited.get(`${waiterThreadId}:${threadId}`)),
      }),
    ),
    Layer.provideMerge(
      Layer.mergeAll(
        layerDatabase,
        ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
        EffectOutbox.layer.pipe(Layer.provide(layerDatabase)),
        ProviderReplayHarness.layerWithRegistry(
          { name: "captain-wake" },
          ProviderAdapterRegistry.layerFromAdapters([adapter]),
          { databaseLayer: layerDatabase, runEffectWorker: false },
        ),
      ),
    ),
  );
};

const makeScenario = Effect.fn("makeScenario")(function* (name: string) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const reactor = yield* CaptainWakeReactor.CaptainWakeReactor;
  const eventSink = yield* EventSink.EventSinkV2;
  const projectId = ProjectId.make(`project:${name}`);
  const captainId = ThreadId.make(`thread:${name}:captain`);
  const now = yield* DateTime.now;
  let sequence = 0;

  const createThread = (
    threadId: ThreadId,
    title: string,
    captain: boolean,
    launchedByThreadId?: ThreadId,
  ) =>
    orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`create:${threadId}`),
      threadId,
      projectId,
      title,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: captain ? "user" : "agent",
      creationSource: captain ? "web" : "mcp",
      ...(captain ? { captain: true } : {}),
      ...(launchedByThreadId === undefined ? {} : { launchedByThreadId }),
    });

  /** Shows the reactor an event the way the event stream would. */
  const publish = Effect.fn("publish")(function* (event: OrchestrationV2DomainEvent) {
    yield* eventSink.write({ events: [event] });
    yield* reactor.handle(event);
  });

  const nextEventId = () => {
    sequence += 1;
    return EventId.make(`event:${name}:${sequence}`);
  };

  /** Starts a run of T3 Code's own: a delegated task's result, or a pull request watch wake. */
  const followUp = Effect.fn("followUp")(function* (
    threadId: ThreadId,
    kind: "delegated_result" | "monitor",
  ) {
    sequence += 1;
    const messageId = MessageId.make(`message:${name}:${sequence}`);
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`follow-up:${name}:${sequence}`),
      threadId,
      messageId,
      text: kind === "monitor" ? "Checks passed" : "The task finished",
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "agent",
      creationSource: "server",
      ...(kind === "monitor"
        ? {
            notification: {
              source: { kind: "monitor" as const },
              outcome: "updated" as const,
              summary: "Checks passed",
            },
          }
        : {}),
    });
    const { runs } = yield* projections.getThreadRecords(threadId, ["runs"]);
    const run = runs.find((candidate) => candidate.userMessageId === messageId)!;
    if (kind === "delegated_result") {
      // Real deliveries need a claimed task; the reactor only reads the stamp they leave.
      const message = (yield* projections.getMessagesById(threadId, [messageId])).get(messageId)!;
      yield* eventSink.write({
        events: [
          {
            id: nextEventId(),
            type: "message.updated",
            threadId,
            occurredAt: now,
            payload: {
              ...message,
              delegatedCompletion: { parentRunId: run.id, generation: 1, taskIds: [] },
            },
          },
        ],
      });
    }
    return run;
  });

  const pullRequest = (watching: boolean): ThreadPullRequestLink => {
    const watch = {
      startedAt: DateTime.formatIso(now),
      headSha: null,
      failedChecks: [],
      passed: false,
      passedChecks: [],
      remarksThrough: DateTime.formatIso(now),
      remarkIds: [],
      conflicting: false,
      wakes: 0,
    };
    return {
      host: "github.com",
      repository: "example/repo",
      number: 7,
      url: "https://github.com/example/repo/pull/7",
      source: "agent",
      linkedAt: DateTime.formatIso(now),
      snapshot: null,
      stack: null,
      ...(watching ? { watch } : { endedWatch: watch }),
    };
  };

  /** Links a pull request the thread watches. */
  const watchPullRequest = Effect.fn("watchPullRequest")(function* (threadId: ThreadId) {
    const thread = yield* projections.getThread(threadId);
    yield* publish({
      id: nextEventId(),
      type: "thread.pull-request-synced",
      threadId,
      occurredAt: now,
      payload: { ...thread, pullRequests: [pullRequest(true)] },
    });
  });

  const endPullRequestWatch = (threadId: ThreadId) =>
    publish({
      id: nextEventId(),
      type: "thread.pull-request-link-updated",
      threadId,
      occurredAt: now,
      payload: pullRequest(false),
    });

  /** Sends a message and returns the run it started. */
  const send = Effect.fn("send")(function* (threadId: ThreadId, from: ThreadId | "user") {
    sequence += 1;
    const messageId = MessageId.make(`message:${name}:${sequence}`);
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`send:${name}:${sequence}`),
      threadId,
      messageId,
      text: "Do the work",
      attachments: [],
      dispatchMode: { type: "start_immediately" },
      ...(from === "user"
        ? { createdBy: "user" as const, creationSource: "web" as const }
        : { createdBy: "agent" as const, creationSource: "mcp" as const, senderThreadId: from }),
    });
    return (yield* projections.getThreadRecords(threadId, ["runs"])).runs.find(
      (run) => run.userMessageId === messageId,
    )!;
  });

  /** Ends a run the way its provider would, and shows the reactor the event. */
  const end = Effect.fn("end")(function* (
    run: OrchestrationV2Run,
    status: "completed" | "failed" = "completed",
  ) {
    sequence += 1;
    const event = {
      id: EventId.make(`event:${name}:${sequence}`),
      type: "run.updated",
      threadId: run.threadId,
      runId: run.id,
      occurredAt: now,
      payload: { ...run, status, startedAt: now, completedAt: now },
    } satisfies OrchestrationV2DomainEvent;
    yield* eventSink.write({ events: [event] });
    yield* reactor.handle(event);
  });

  const wakes = Effect.fn("wakes")(function* () {
    yield* TestClock.adjust(CaptainWakeReactor.CAPTAIN_WAKE_DEBOUNCE);
    const { messages, runs } = yield* projections.getThreadRecords(captainId, ["messages", "runs"]);
    return messages
      .filter((message) => message.notification !== undefined)
      .map((message) => ({
        text: message.text,
        summary: message.notification!.summary,
        run: runs.find((run) => run.userMessageId === message.id)!,
      }));
  });

  yield* createThread(captainId, "Captain", true);
  const child = (suffix: string, options: { readonly launchedByCaptain?: boolean } = {}) =>
    Effect.gen(function* () {
      const threadId = ThreadId.make(`thread:${name}:${suffix}`);
      yield* createThread(
        threadId,
        `Work ${suffix}`,
        false,
        options.launchedByCaptain === true ? captainId : undefined,
      );
      return threadId;
    });
  return {
    projections,
    reactor,
    eventSink,
    captainId,
    now,
    child,
    send,
    followUp,
    watchPullRequest,
    endPullRequestWatch,
    end,
    wakes,
  };
});

/** Ends the wake-up turns so the next update reaches an idle captain. */
const endWakes = Effect.fn("endWakes")(function* (
  scenario: Effect.Success<ReturnType<typeof makeScenario>>,
) {
  const wakes = yield* scenario.wakes();
  for (const wake of wakes) {
    if (wake.run.status !== "completed") yield* scenario.end(wake.run);
  }
  return wakes;
});

it.effect("wakes the captain once for work it asked for, after its own turn ends", () =>
  Effect.gen(function* () {
    const scenario = yield* makeScenario("batch");
    const first = yield* scenario.child("a");
    const second = yield* scenario.child("b");
    const own = yield* scenario.child("own");

    const captainTurn = yield* scenario.send(scenario.captainId, "user");
    yield* scenario.end(yield* scenario.send(first, scenario.captainId));
    yield* scenario.end(yield* scenario.send(second, scenario.captainId), "failed");
    // The user's own thread never reports to the captain.
    yield* scenario.end(yield* scenario.send(own, "user"));
    assert.deepEqual(yield* scenario.wakes(), []);

    yield* scenario.end(captainTurn);
    const wakes = yield* scenario.wakes();
    assert.deepEqual(
      wakes.map((wake) => wake.summary),
      ["2 threads have updates"],
    );
    assert.include(wakes[0]!.text, `"Work a" (threadId ${first}) finished.`);
    assert.include(wakes[0]!.text, `"Work b" (threadId ${second}) failed.`);
    assert.notInclude(wakes[0]!.text, "Work own");
    assert.include(wakes[0]!.text, "reply exactly (no update)");

    // A later turn the user started in that thread is theirs, not the captain's.
    yield* scenario.end(wakes[0]!.run);
    yield* scenario.end(yield* scenario.send(first, "user"));
    assert.lengthOf(yield* scenario.wakes(), 1);
  }).pipe(Effect.provide(layerTest())),
);

it.effect("skips a result the captain already waited for", () =>
  Effect.gen(function* () {
    const scenario = yield* makeScenario("waited");
    const threadId = yield* scenario.child("a");
    const run = yield* scenario.send(threadId, scenario.captainId);
    waited.set(`${scenario.captainId}:${threadId}`, run.id);
    yield* scenario.end(run);
    assert.deepEqual(yield* scenario.wakes(), []);

    const next = yield* scenario.send(threadId, scenario.captainId);
    yield* scenario.end(next);
    assert.deepEqual(
      (yield* scenario.wakes()).map((wake) => wake.summary),
      ['"Work a" finished'],
    );
  }).pipe(Effect.provide(layerTest())),
);

it.effect("tells the captain about a question that is still open", () =>
  Effect.gen(function* () {
    const scenario = yield* makeScenario("question");
    const threadId = yield* scenario.child("a");
    const run = yield* scenario.send(threadId, scenario.captainId);
    const question = (id: string, status: "pending" | "resolved") =>
      ({
        id: EventId.make(`event:question:${id}:${status}`),
        type: "runtime-request.updated",
        threadId,
        runId: run.id,
        occurredAt: scenario.now,
        payload: {
          id: RuntimeRequestId.make(`request:${id}`),
          nodeId: run.rootNodeId!,
          providerTurnId: null,
          nativeRequestRef: null,
          kind: "user_input",
          status,
          responseCapability: {
            type: "live",
            providerSessionId: ProviderSessionId.make("session:question"),
          },
          createdAt: scenario.now,
          resolvedAt: status === "resolved" ? scenario.now : null,
        },
      }) satisfies OrchestrationV2DomainEvent;

    // Answered before the captain is told: nothing to say.
    yield* scenario.eventSink.write({ events: [question("answered", "pending")] });
    yield* scenario.reactor.handle(question("answered", "pending"));
    yield* scenario.eventSink.write({ events: [question("answered", "resolved")] });
    assert.deepEqual(yield* scenario.wakes(), []);

    yield* scenario.eventSink.write({ events: [question("open", "pending")] });
    yield* scenario.reactor.handle(question("open", "pending"));
    const wakes = yield* scenario.wakes();
    assert.deepEqual(
      wakes.map((wake) => wake.summary),
      ['"Work a" has a question'],
    );
    assert.include(wakes[0]!.text, "requestId request:open");
  }).pipe(Effect.provide(layerTest())),
);

it.effect("pauses wake-ups after too many in a row until the user writes", () =>
  Effect.gen(function* () {
    const scenario = yield* makeScenario("pause");
    const threadId = yield* scenario.child("a");
    const report = Effect.gen(function* () {
      yield* scenario.end(yield* scenario.send(threadId, scenario.captainId));
      const wakes = yield* scenario.wakes();
      const latest = wakes.at(-1)!;
      if (latest.run.status !== "completed") yield* scenario.end(latest.run);
      return wakes;
    });

    let wakes = yield* report;
    for (let index = 1; index < CaptainWakeReactor.MAX_UNATTENDED_CAPTAIN_WAKES; index += 1) {
      wakes = yield* report;
    }
    assert.lengthOf(wakes, CaptainWakeReactor.MAX_UNATTENDED_CAPTAIN_WAKES);
    assert.include(wakes.at(-1)!.text, "last automatic wake-up");
    assert.notInclude(wakes.at(-2)!.text, "last automatic wake-up");

    assert.lengthOf(yield* report, CaptainWakeReactor.MAX_UNATTENDED_CAPTAIN_WAKES);

    // The user writing lifts the pause, and the held update goes out after that turn.
    const userTurn = yield* scenario.send(scenario.captainId, "user");
    const created = {
      id: EventId.make("event:pause:user-turn"),
      type: "run.created",
      threadId: scenario.captainId,
      runId: userTurn.id,
      occurredAt: scenario.now,
      payload: userTurn,
    } satisfies OrchestrationV2DomainEvent;
    yield* scenario.reactor.handle(created);
    yield* scenario.end(userTurn);
    assert.lengthOf(yield* scenario.wakes(), CaptainWakeReactor.MAX_UNATTENDED_CAPTAIN_WAKES + 1);
  }).pipe(Effect.provide(layerTest())),
);

it.effect("follows a delegated result back to the captain that asked for the work", () =>
  Effect.gen(function* () {
    const scenario = yield* makeScenario("delegated");
    const threadId = yield* scenario.child("a");
    yield* scenario.end(yield* scenario.send(threadId, scenario.captainId));
    assert.lengthOf(yield* endWakes(scenario), 1);

    // The thread delegated a review and continued from its result.
    const result = yield* scenario.followUp(threadId, "delegated_result");
    yield* scenario.end(result);
    const wakes = yield* endWakes(scenario);
    assert.deepEqual(
      wakes.map((wake) => wake.summary),
      ['"Work a" finished', '"Work a" finished'],
    );

    // A pull request wake after that result has nothing new to report.
    yield* scenario.end(yield* scenario.followUp(threadId, "monitor"));
    assert.lengthOf(yield* scenario.wakes(), 2);
  }).pipe(Effect.provide(layerTest())),
);

it.effect("waits for the thread's pull request watch to end", () =>
  Effect.gen(function* () {
    const scenario = yield* makeScenario("watch");
    const threadId = yield* scenario.child("a");
    const run = yield* scenario.send(threadId, scenario.captainId);
    yield* scenario.watchPullRequest(threadId);
    yield* scenario.end(run);
    assert.deepEqual(yield* scenario.wakes(), []);

    // The watch wakes the thread, and that turn ends with the watch still open.
    yield* scenario.end(yield* scenario.followUp(threadId, "monitor"));
    assert.deepEqual(yield* scenario.wakes(), []);

    // The pull request merges: the watch ends with no run left to end.
    yield* scenario.endPullRequestWatch(threadId);
    assert.deepEqual(
      (yield* scenario.wakes()).map((wake) => wake.summary),
      ['"Work a" finished'],
    );
  }).pipe(Effect.provide(layerTest())),
);

it.effect("reports work the captain launched past the follow-ups it reads", () =>
  Effect.gen(function* () {
    const scenario = yield* makeScenario("launched");
    const threadId = yield* scenario.child("a", { launchedByCaptain: true });
    yield* scenario.end(yield* scenario.send(threadId, scenario.captainId));
    yield* endWakes(scenario);
    for (let index = 0; index < 24; index += 1) {
      yield* scenario.end(yield* scenario.followUp(threadId, "delegated_result"));
    }
    // Results that pile up while the captain waits are one line about the thread.
    const piled = yield* endWakes(scenario);
    assert.lengthOf(piled, 2);
    assert.equal(piled[1]!.summary, '"Work a" finished');

    // This run's follow-ups reach back further than the captain's message is read for.
    yield* scenario.end(yield* scenario.followUp(threadId, "delegated_result"));
    assert.lengthOf(yield* endWakes(scenario), 3);

    // The user taking the thread over still ends the captain's claim on it.
    yield* scenario.end(yield* scenario.send(threadId, "user"));
    yield* scenario.end(yield* scenario.followUp(threadId, "delegated_result"));
    assert.lengthOf(yield* scenario.wakes(), 3);
  }).pipe(Effect.provide(layerTest())),
);

it.effect("stays quiet when wake-ups are turned off", () =>
  Effect.gen(function* () {
    const scenario = yield* makeScenario("off");
    const threadId = yield* scenario.child("a");
    yield* scenario.end(yield* scenario.send(threadId, scenario.captainId));
    assert.deepEqual(yield* scenario.wakes(), []);
  }).pipe(Effect.provide(layerTest({ captainWakes: false }))),
);
