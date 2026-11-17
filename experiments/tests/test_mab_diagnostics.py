from collections import Counter
import unittest

from experiments.analysis.mab_diagnostics import (
    BACKENDS, check_bandit, feedback_reward, phase, prior_feedback_mass, queue_by_burst, selection_winner,
)


def arm(backend, score=0, observed=False):
    return {"backend_id": backend, "score": score, "observed": observed,
            "mean_reward": .5 if observed else 0, "effective_count": int(observed)}


class BanditDiagnosticTest(unittest.TestCase):
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
