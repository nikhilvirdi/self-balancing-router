from collections import Counter
from copy import deepcopy
from math import fsum, log, sqrt
from sys import float_info
import unittest

from experiments.analysis.mab_diagnostics import (
    BACKENDS, PENDING_POLICY, check_bandit, feedback_reward, phase, prior_feedback_mass, queue_by_burst, replay_settings, selection_winner,
)


def arm(backend, score=0, observed=False):
    return {"backend_id": backend, "score": score, "observed": observed,
            "mean_reward": .5 if observed else 0, "effective_count": int(observed)}


def pending_history(samples, window_size=1000, scale_ms=100, eligible=None):
    """Build serial decision records from retained source rewards, without rolling sums."""
    backends = list(dict.fromkeys(backend for backend, _, _ in samples))
    history, rows = [], []
    for i, (backend, duration, status) in enumerate(samples):
        order = 2*i+1
        arms = []
        candidates = backends if eligible is None else eligible[i]
        for candidate in candidates:
            values = [reward for b, reward in history if b == candidate]
            count = len(values)
            mean = fsum(values)/count if count else 0
            score = mean + sqrt(2*log(max(1, len(history)))/count) if count else 0
            arms.append({"backend_id": candidate, "effective_count": count, "mean_reward": mean,
                         "score": score, "observed": bool(count),
                         "pending": {"count": 0, "penalty": 0, "adjusted_score": score}})
            if not count:
                break
        selected = {"backend_id": backend, "request_id": str(i+1),
                    "in_flight": [{"BackendID": b, "Count": 0} for b in backends],
                    "bandit": {"order": order, "eligible": candidates, "arms": arms,
                               "reason": "cold_no_pending" if not arms[-1]["observed"] else "maximum_ucb"}}
        completed = {"request_id": str(i+1), "backend_id": backend, "status": status,
                     "outcome": "upstream_response", "duration_ns": duration}
        reward = feedback_reward(completed, scale_ms)
        feedback = {"request_id": str(i+1), "backend_id": backend, "feedback": {
            "order": order+1, "age_ns": duration, "upstream_status": status, "applied": True, "reward": reward}}
        rows.append((0, selected, {"request_completed": (0, completed), "bandit_feedback": (0, feedback)}))
        history = (history + [(backend, reward)])[-window_size:]
    return rows, backends


class BanditDiagnosticTest(unittest.TestCase):
    def test_pending_scores_negative_ties_and_overflow(self):
        for coefficient, loads, means, winner in (
            (.05, (3, 0), (.85, .65), "a"),
            (.05, (5, 0), (.85, .65), "b"),
            (1, (5, 4), (.85, .65), "b"),
            (.5, (1, 1), (.5, .5), "a"),
            (float_info.max, (3, 2), (.85, .65), "b"),
            (float_info.max, (2, 2), (.65, .85), "b"),
        ):
            decision = {"eligible": ["a", "b"], "arms": []}
            pending = dict(zip(("a", "b"), loads))
            for backend, count, mean in zip(("a", "b"), loads, means):
                score = mean + sqrt(2*log(200)/100)
                penalty = min(float_info.max, coefficient*count)
                decision["arms"].append({
                    "backend_id": backend, "score": score, "mean_reward": mean, "effective_count": 100,
                    "observed": True, "pending": {"count": count, "penalty": penalty, "adjusted_score": score-penalty,
                                                  "saturated": coefficient*count > float_info.max},
                })
            with self.subTest(coefficient=coefficient, loads=loads):
                self.assertEqual(selection_winner(decision, pending, coefficient), (winner, "maximum_ucb"))
                for field, value in (("penalty", 7), ("adjusted_score", float("nan")), ("count", -1),
                                     ("saturated", 1)):
                    corrupted = deepcopy(decision)
                    corrupted["arms"][0]["pending"][field] = value
                    with self.assertRaises(ValueError):
                        selection_winner(corrupted, pending, coefficient)
                corrupted = deepcopy(decision)
                corrupted["arms"].pop()
                with self.assertRaises(ValueError):
                    selection_winner(corrupted, pending, coefficient)

    def test_pending_replay_configuration(self):
        settings = {"SBR_PROXY_MAB_PENDING_PENALTY": "0.05", "SBR_PROXY_MAB_LATENCY_SCALE": "1s500ms",
                    "SBR_PROXY_MAB_SW_WINDOW": "1"}
        meta = {"routing_policy": PENDING_POLICY, "configuration": {"policy_configuration": settings}}
        self.assertEqual(replay_settings(meta), (1500, 1, .05))
        for key, value in (("SBR_PROXY_MAB_PENDING_PENALTY", "-1"), ("SBR_PROXY_MAB_PENDING_PENALTY", "NaN"),
                           ("SBR_PROXY_MAB_PENDING_PENALTY", True),
                           ("SBR_PROXY_MAB_SW_WINDOW", "0"), ("SBR_PROXY_MAB_SW_WINDOW", 1.9),
                           ("SBR_PROXY_MAB_SW_WINDOW", True), ("SBR_PROXY_MAB_LATENCY_SCALE", "0ms")):
            corrupted = deepcopy(meta)
            corrupted["configuration"]["policy_configuration"][key] = value
            with self.assertRaises(ValueError):
                replay_settings(corrupted)

    def test_pending_replay_uses_reconstructed_ranking(self):
        samples = [("a", 1_000_000_000, 200), ("b", 1_000_000_001, 200), ("a", 0, 200)]
        rows, backends = pending_history(samples)
        check_bandit(rows, PENDING_POLICY, 100, pending_penalty=.05, backend_ids=backends)
        corrupt = deepcopy(rows)
        selected, trace = corrupt[-1][1:]
        selected["backend_id"] = trace["request_completed"][1]["backend_id"] = trace["bandit_feedback"][1]["backend_id"] = "b"
        candidate = selected["bandit"]["arms"][1]
        candidate["score"] += 5e-10
        candidate["pending"]["adjusted_score"] = candidate["score"]
        with self.assertRaisesRegex(ValueError, "selected backend"):
            check_bandit(corrupt, PENDING_POLICY, 100, pending_penalty=.05, backend_ids=backends)

    def test_pending_replay_retained_reward_precision(self):
        samples = [("a", 0, 200), ("a", 2**63-1, 200), ("a", 0, 503),
                   ("b", 0, 503), ("b", 0, 503), ("a", 0, 200)]
        rows, backends = pending_history(samples, window_size=3, scale_ms=.000001,
                                        eligible=[["a"]]*3 + [["a", "b"]]*3)
        check_bandit(rows, PENDING_POLICY, .000001, window_size=3, pending_penalty=.05, backend_ids=backends)

    def test_pending_feedback_replay_and_corruption(self):
        backend = "a"
        def decision(order, load, observed):
            score = .5 if observed else 0
            return {"backend_id": backend, "request_id": str(order), "in_flight": [{"BackendID": backend, "Count": load}],
                    "bandit": {"order": order, "reason": "maximum_ucb" if observed else "cold_no_pending",
                               "eligible": [backend], "arms": [{
                                   "backend_id": backend, "score": score, "mean_reward": score,
                                   "effective_count": int(observed), "observed": observed,
                                   "pending": {"count": load, "penalty": load*2, "adjusted_score": score-load*2}}]}}
        def trace(selected, order, status=200, outcome="upstream_response"):
            return {"request_completed": (1, {"request_id": selected["request_id"], "backend_id": backend, "status": status,
                                              "outcome": outcome, "duration_ns": 100_000_000}),
                    "bandit_feedback": (1, {"request_id": selected["request_id"], "backend_id": backend, "feedback": {
                        "order": order, "age_ns": 100_000_001, "reward": .5, "applied": True, "upstream_status": status}})}
        first = decision(1, 0, False)
        second = decision(3, 0, True)
        third = decision(4, 1, True)
        rows = [(0, first, trace(first, 2)), (0, second, trace(second, 6)), (0, third, trace(third, 5))]
        check_bandit(rows, PENDING_POLICY, 100, window_size=1, pending_penalty=2, backend_ids=[backend])
        mutations = (
            lambda r: r[1][2]["bandit_feedback"][1]["feedback"].update(reward=.6),
            lambda r: r[1][1]["bandit"]["arms"][0].update(score=.6),
            lambda r: r[2][1]["in_flight"][0].update(Count=0),
            lambda r: r[2][1]["bandit"]["arms"][0]["pending"].update(adjusted_score=0),
            lambda r: r[2][1]["bandit"].update(eligible=[]),
            lambda r: r[2][1]["bandit"].update(reason="cold_least_pending"),
            lambda r: r[2][2]["bandit_feedback"][1]["feedback"].update(order=6),
            lambda r: r[2][2]["request_completed"][1].update(request_id="wrong"),
            lambda r: r[2][2]["request_completed"][1].update(status=503),
            lambda r: r[2][2]["request_completed"][1].update(outcome="unknown"),
            lambda r: r[2][2]["request_completed"][1].update(backend_id="other"),
            lambda r: r[2][2]["bandit_feedback"][1]["feedback"].update(upstream_status=99, reward=0, applied=False),
            lambda r: [event.update(request_id="1") for event in (
                r[2][1], r[2][2]["request_completed"][1], r[2][2]["bandit_feedback"][1])],
            lambda r: r[2][2]["bandit_feedback"][1]["feedback"].update(applied=False),
            lambda r: r[2][1]["bandit"]["arms"][0].pop("pending"),
            lambda r: r[2][1]["bandit"]["arms"][0].update(effective_count=1.0000000001),
            lambda r: r[2][1]["bandit"]["arms"][0].update(effective_count=True),
        )
        for mutate in mutations:
            corrupted = deepcopy(rows)
            mutate(corrupted)
            with self.assertRaises((ValueError, KeyError)):
                check_bandit(corrupted, PENDING_POLICY, 100, window_size=1, pending_penalty=2, backend_ids=[backend])
        instant, ids = pending_history([("a", 0, 200)])
        instant[0][2]["bandit_feedback"][1]["feedback"]["reward"] = True
        with self.assertRaises(ValueError):
            check_bandit(instant, PENDING_POLICY, 100, pending_penalty=.05, backend_ids=ids)
        self.assertIsNone(feedback_reward({"status": 502, "outcome": "transport_failure"}, 100, 200))
        self.assertEqual(feedback_reward({"status": 502, "outcome": "transport_failure"}, 100, 0), 0)
        for status, outcome, upstream, valid in (
            (503, "upstream_response", 503, True), (502, "transport_failure", 0, True),
            (504, "upstream_timeout", 0, True), (302, "upstream_response", 302, False),
            (400, "upstream_response", 400, False), (0, "canceled", 0, False),
            (200, "interrupted", 200, False), (502, "transport_failure", 200, False),
        ):
            selected = decision(1, 0, False)
            terminal = trace(selected, 2, status, outcome)
            terminal["bandit_feedback"][1]["feedback"].update(reward=0, applied=valid, upstream_status=upstream)
            check_bandit([(0, selected, terminal)], PENDING_POLICY, 100, window_size=1,
                         pending_penalty=2, backend_ids=[backend])
            if not valid:
                next_selected = decision(3, 0, False)
                check_bandit([(0, selected, terminal), (1, next_selected, trace(next_selected, 4))],
                             PENDING_POLICY, 100, window_size=1, pending_penalty=2, backend_ids=[backend])

    def test_score_ties_and_cold_arms(self):
        pending = Counter({"backend-a": 1})
        self.assertEqual(selection_winner({"arms": [arm("backend-a"), arm("backend-b")]}, pending),
                         ("backend-b", "cold_no_pending"))
        self.assertEqual(selection_winner({"arms": [arm("backend-a", 4, True), arm("backend-b", 4, True)]}, pending),
                         ("backend-a", "maximum_ucb"))
        self.assertEqual(selection_winner({"arms": [arm("backend-a")]}, pending), ("backend-a", "cold_least_pending"))

    def test_feedback_attribution_and_exact_order(self):
        completion = {"status": 200, "outcome": "upstream_response", "duration_ns": 100_000_000}
        self.assertEqual(feedback_reward(completion, 100), .5)
        self.assertEqual(feedback_reward({**completion, "status": 503}, 100), 0)
        self.assertIsNone(feedback_reward({**completion, "outcome": "canceled"}, 100))
        selected = {"backend_id": "backend-a", "in_flight": [{"BackendID": b, "Count": 0} for b in BACKENDS],
                    "bandit": {"reason": "cold_no_pending", "order": 1, "arms": [arm("backend-a")]}}
        applied = {"backend_id": "backend-a", "feedback": {"order": 2, "applied": True, "reward": .5, "age_ns": 100_010_000}}
        traces = [(0, selected, {"bandit_feedback": (.1, applied), "request_completed": (.1, completion)})]
        check_bandit(traces, "ucb1", 100)
        second = {**selected, "bandit": {"reason": "maximum_ucb", "order": 3,
                                        "arms": [arm("backend-a", .5, True)]}}
        next_feedback = {**applied, "feedback": {**applied["feedback"], "order": 4}}
        traces.append((.2, second, {"bandit_feedback": (.3, next_feedback), "request_completed": (.3, completion)}))
        for policy in ("ucb1", "sliding-window-ucb", "discounted-ucb"):
            check_bandit(traces, policy, 100)
        second["bandit"]["arms"][0]["score"] = .6
        with self.assertRaisesRegex(ValueError, "Recorded score"):
            check_bandit(traces, "ucb1", 100)
        traces.pop()
        selected["in_flight"][0]["Count"] = 1
        with self.assertRaisesRegex(ValueError, "Pending reservations"):
            check_bandit(traces, "ucb1", 100)

    def test_bursts_and_previous_condition_mass(self):
        selections = [(i, {"backend_id": "backend-a"}, {"queue_admitted": (i, {"queued": i})}) for i in range(8)]
        burst, = queue_by_burst(selections)
        self.assertEqual((burst["length"], burst["queue_first"], burst["queue_max"]), (8, 0, 7))
        self.assertEqual(phase(10, [10, 20]), "degraded")
        history = [(1, 0, "backend-a"), (2, 1, "backend-a"), (3, 5, "backend-b")]
        self.assertEqual(prior_feedback_mass("ucb1", history, 4, .5), (2, .5))
        self.assertLess(prior_feedback_mass("discounted-ucb", history, 4, .5)[1], .5)
        history += [(i, i, "backend-a") for i in range(4, 1004)]
        self.assertEqual(prior_feedback_mass("sliding-window-ucb", history, 1004, .5), (1000, 0))
