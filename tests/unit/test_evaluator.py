import unittest
from vespa.evaluation import VespaEvaluator
from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class MockVespaResponse:
    """Mock Vespa query response"""

    hits: List[Dict[str, Any]]

    def get_json(self):
        return {"root": {"children": self.hits}}


class TestVespaEvaluator(unittest.TestCase):
    def setUp(self):
        # Sample queries
        self.queries = {
            "q1": "what is machine learning",
            "q2": "how to code python",
            "q3": "what is the capital of France",
        }

        # Sample relevant docs
        self.relevant_docs = {
            "q1": {"doc1", "doc2", "doc3"},
            "q2": {"doc4", "doc5"},
            "q3": {"doc6"},
        }

        self.relevant_docs_single = {
            "q1": "doc1",
            "q2": "doc4",
            "q3": "doc6",
        }

        # Mock Vespa responses
        # For q1: doc1 at rank 1, doc2 at rank 3, doc3 at rank 5
        q1_response = MockVespaResponse(
            [
                {"id": "doc1", "relevance": 0.9},
                {"id": "doc10", "relevance": 0.8},
                {"id": "doc2", "relevance": 0.7},
                {"id": "doc11", "relevance": 0.6},
                {"id": "doc3", "relevance": 0.5},
            ]
        )

        # For q2: doc4 at rank 2, doc5 at rank 4
        q2_response = MockVespaResponse(
            [
                {"id": "doc12", "relevance": 0.95},
                {"id": "doc4", "relevance": 0.85},
                {"id": "doc13", "relevance": 0.75},
                {"id": "doc5", "relevance": 0.65},
                {"id": "doc14", "relevance": 0.55},
            ]
        )
        # For q3: doc6 at rank 1
        q3_response = MockVespaResponse(
            [
                {"id": "doc6", "relevance": 0.9},
                {"id": "doc16", "relevance": 0.8},
                {"id": "doc17", "relevance": 0.7},
                {"id": "doc18", "relevance": 0.6},
                {"id": "doc19", "relevance": 0.5},
            ]
        )

        class MockVespaApp:
            def __init__(self, mock_responses):
                self.mock_responses = mock_responses
                self.current_query = 0

            def query(self, body):
                response = self.mock_responses[self.current_query]
                self.current_query = (self.current_query + 1) % len(self.mock_responses)
                return response

        self.mock_app = MockVespaApp([q1_response, q2_response, q3_response])

        def mock_vespa_query_fn(query_text: str, top_k: int) -> dict:
            return {
                "yql": f'select * from sources * where text contains "{query_text}";',
                "hits": top_k,
            }

        self.vespa_query_fn = mock_vespa_query_fn

    def test_basic_initialization(self):
        """Test basic initialization with default parameters"""
        evaluator = VespaEvaluator(
            queries=self.queries,
            relevant_docs=self.relevant_docs,
            vespa_query_fn=self.vespa_query_fn,
            app=self.mock_app,
        )

        self.assertEqual(len(evaluator.queries_ids), 3)
        self.assertEqual(set(evaluator.queries_ids), {"q1", "q2", "q3"})
        self.assertEqual(evaluator.accuracy_at_k, [1, 3, 5, 10])
        self.assertEqual(evaluator.precision_recall_at_k, [1, 3, 5, 10])
        self.assertEqual(evaluator.mrr_at_k, [10])
        self.assertEqual(evaluator.ndcg_at_k, [10])
        self.assertEqual(evaluator.map_at_k, [100])

    def test_init_single_relevant_docs(self):
        """Test initialization with single relevant doc per query"""
        evaluator = VespaEvaluator(
            queries=self.queries,
            relevant_docs=self.relevant_docs_single,
            vespa_query_fn=self.vespa_query_fn,
            app=self.mock_app,
        )
        relevant_docs_to_set = {  # Convert to the same format as self.relevant_docs
            q_id: {doc_id} for q_id, doc_id in self.relevant_docs_single.items()
        }
        self.assertEqual(evaluator.relevant_docs, relevant_docs_to_set)

    def test_custom_k_values(self):
        """Test initialization with custom k values"""
        evaluator = VespaEvaluator(
            queries=self.queries,
            relevant_docs=self.relevant_docs,
            vespa_query_fn=self.vespa_query_fn,
            app=self.mock_app,
            accuracy_at_k=[1, 2],
            precision_recall_at_k=[1, 2, 3],
            mrr_at_k=[5],
            ndcg_at_k=[5],
            map_at_k=[5],
        )

        self.assertEqual(evaluator.accuracy_at_k, [1, 2])
        self.assertEqual(evaluator.precision_recall_at_k, [1, 2, 3])
        self.assertEqual(evaluator.mrr_at_k, [5])
        self.assertEqual(evaluator.ndcg_at_k, [5])
        self.assertEqual(evaluator.map_at_k, [5])

    def test_accuracy_metrics(self):
        """Test accuracy@k calculations"""
        evaluator = VespaEvaluator(
            queries=self.queries,
            relevant_docs=self.relevant_docs,
            vespa_query_fn=self.vespa_query_fn,
            app=self.mock_app,
            accuracy_at_k=[1, 3, 5],
            precision_recall_at_k=[],
            mrr_at_k=[],
            ndcg_at_k=[],
            map_at_k=[],
        )

        results = evaluator.run()

        self.assertAlmostEqual(results["accuracy@1"], 2 / 3)  # q1 and q3 hit at 1
        self.assertAlmostEqual(results["accuracy@3"], 1.0)
        self.assertAlmostEqual(results["accuracy@5"], 1.0)

    def test_precision_recall_metrics(self):
        """Test precision@k and recall@k calculations"""
        evaluator = VespaEvaluator(
            queries=self.queries,
            relevant_docs=self.relevant_docs,
            vespa_query_fn=self.vespa_query_fn,
            app=self.mock_app,
            accuracy_at_k=[],
            precision_recall_at_k=[3, 5],
            mrr_at_k=[],
            ndcg_at_k=[],
            map_at_k=[],
        )

        results = evaluator.run()

        self.assertAlmostEqual(results["precision@3"], (2 / 3 + 1 / 3 + 1 / 3) / 3)
        self.assertAlmostEqual(results["recall@3"], (2 / 3 + 1 / 2 + 1 / 1) / 3)
        self.assertAlmostEqual(results["precision@5"], (3 / 5 + 2 / 5 + 1 / 5) / 3)
        self.assertAlmostEqual(results["recall@5"], (3 / 3 + 2 / 2 + 1 / 1) / 3)

    def test_mrr_metric(self):
        """Test MRR@k calculations"""
        evaluator = VespaEvaluator(
            queries=self.queries,
            relevant_docs=self.relevant_docs,
            vespa_query_fn=self.vespa_query_fn,
            app=self.mock_app,
            accuracy_at_k=[],
            precision_recall_at_k=[],
            mrr_at_k=[5],
            ndcg_at_k=[],
            map_at_k=[],
        )

        results = evaluator.run()
        expected_mrr = (
            1 + (1 / 2) + 1
        ) / 3  # q1 first at 1, q2 first at 2, q3 first at 1
        self.assertAlmostEqual(results["mrr@5"], expected_mrr)

    def test_ndcg_metric(self):
        """Test NDCG@k calculations"""
        evaluator = VespaEvaluator(
            queries=self.queries,
            relevant_docs=self.relevant_docs,
            vespa_query_fn=self.vespa_query_fn,
            app=self.mock_app,
            accuracy_at_k=[],
            precision_recall_at_k=[],
            mrr_at_k=[],
            ndcg_at_k=[5],
            map_at_k=[],
        )

        results = evaluator.run()
        # NDCG@5 calculation:
        # q1: (1/log2(2) + 1/log2(4) + 1/log2(6)) / (1/log2(2) + 1/log2(3) + 1/log2(4))
        # q2: (1/log2(3) + 1/log2(5)) / (1/log2(2) + 1/log2(3))
        # q3: (1/log2(2)) / (1/log2(2))
        # Average of all three queries
        expected_ndcg = 0.8455  # Approximate value
        self.assertAlmostEqual(results["ndcg@5"], expected_ndcg, places=4)

    def test_map_metric(self):
        """Test MAP@k calculations"""
        evaluator = VespaEvaluator(
            queries=self.queries,
            relevant_docs=self.relevant_docs,
            vespa_query_fn=self.vespa_query_fn,
            app=self.mock_app,
            accuracy_at_k=[],
            precision_recall_at_k=[],
            mrr_at_k=[],
            ndcg_at_k=[],
            map_at_k=[5],
        )

        results = evaluator.run()
        # MAP@5 calculation:
        # q1: (1/1 + 2/3 + 3/5) / 3 -> (1 + 0.6667 + 0.6) / 3 -> 2.2667 / 3 -> 0.7556
        # q2: (1/2 + 2/4) / 2 -> (0.5 + 0.5) / 2 -> 1 / 2 -> 0.5
        # q3: (1/1) / 1 -> 1 / 1 -> 1
        # Average of all three queries: (0.7556 + 0.5 + 1) / 3 -> 2.2556 / 3 -> 0.7519
        expected_map = 0.7519  # Approximate value
        self.assertAlmostEqual(results["map@5"], expected_map, places=4)

    def test_vespa_query_fn_validation(self):
        """Test validation of vespa_query_fn with valid functions"""

        # Valid function with type hints
        def fn1(query: str, k: int) -> dict:
            return {"yql": query, "hits": k}

        # Valid function without type hints
        def fn2(query, k):
            return {"yql": query, "hits": k}

        # Valid function with default args
        def fn3(query: str, k: int = 10) -> dict:
            return {"yql": query, "hits": k}

        # All should work without raising exceptions
        for fn in [fn1, fn2, fn3]:
            evaluator = VespaEvaluator(
                queries=self.queries,
                relevant_docs=self.relevant_docs,
                vespa_query_fn=fn,
                app=self.mock_app,
            )
            self.assertIsInstance(evaluator, VespaEvaluator)

    def test_vespa_query_fn_validation_errors(self):
        """Test validation of vespa_query_fn with invalid functions"""

        # Not a callable
        with self.assertRaisesRegex(ValueError, "must be a callable"):
            VespaEvaluator(
                queries=self.queries,
                relevant_docs=self.relevant_docs,
                vespa_query_fn="not_a_function",
                app=self.mock_app,
            )

        # Wrong number of params
        def fn1(query: str) -> dict:
            return {"yql": query}

        with self.assertRaisesRegex(TypeError, "must take exactly 2 parameters"):
            VespaEvaluator(
                queries=self.queries,
                relevant_docs=self.relevant_docs,
                vespa_query_fn=fn1,
                app=self.mock_app,
            )

        # Wrong param types
        def fn2(query: int, k: str) -> dict:
            return {"yql": str(query), "hits": int(k)}

        with self.assertRaisesRegex(TypeError, "must be of type"):
            VespaEvaluator(
                queries=self.queries,
                relevant_docs=self.relevant_docs,
                vespa_query_fn=fn2,
                app=self.mock_app,
            )

        # Wrong return type annotation
        def fn3(query: str, k: int) -> list:
            return [query, k]

        with self.assertRaisesRegex(ValueError, "must return a dict"):
            VespaEvaluator(
                queries=self.queries,
                relevant_docs=self.relevant_docs,
                vespa_query_fn=fn3,
                app=self.mock_app,
            )

        # Function that raises error
        def fn4(query: str, k: int) -> dict:
            raise ValueError("Something went wrong")

        with self.assertRaisesRegex(ValueError, "Error calling vespa_query_fn"):
            VespaEvaluator(
                queries=self.queries,
                relevant_docs=self.relevant_docs,
                vespa_query_fn=fn4,
                app=self.mock_app,
            )

        # Function that returns wrong type at runtime
        def fn5(query: str, k: int) -> dict:
            return [query, k]  # Actually returns a list

        with self.assertRaisesRegex(ValueError, "must return a dict"):
            VespaEvaluator(
                queries=self.queries,
                relevant_docs=self.relevant_docs,
                vespa_query_fn=fn5,
                app=self.mock_app,
            )


if __name__ == "__main__":
    unittest.main()
