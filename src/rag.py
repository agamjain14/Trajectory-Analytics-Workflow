"""
RAG (Retrieval-Augmented Generation) module.
Uses ChromaDB for vector storage and sentence-transformers for embeddings.
Fully instrumented with OpenTelemetry traces and metrics.
"""

import os
import time
from typing import List, Dict, Any

import chromadb
from chromadb.config import Settings
from opentelemetry import trace
from opentelemetry.trace import StatusCode

from src.telemetry import get_tracer, get_logger
from src.metrics import AppMetrics

# Travel knowledge base documents
SAMPLE_DOCUMENTS = [
    {
        "id": "travel-1",
        "text": "Tokyo, Japan is best visited during spring (March-May) for cherry blossoms or autumn (October-November) for fall foliage. The city offers a mix of ultra-modern architecture, traditional temples, and world-class cuisine. Budget around $150-300/day for mid-range travel.",
        "metadata": {"source": "destination-guide", "topic": "tokyo", "region": "asia"},
    },
    {
        "id": "travel-2",
        "text": "Paris, France is ideal year-round but spring and fall avoid peak crowds. Must-see attractions include the Eiffel Tower, Louvre Museum, and Notre-Dame. The metro system is efficient for getting around. Budget $200-400/day for mid-range.",
        "metadata": {"source": "destination-guide", "topic": "paris", "region": "europe"},
    },
    {
        "id": "travel-3",
        "text": "Bali, Indonesia offers tropical beaches, rice terraces, and Hindu temples. Best visited April-October (dry season). Ubud is the cultural heart while Seminyak has beaches. Very affordable at $50-150/day.",
        "metadata": {"source": "destination-guide", "topic": "bali", "region": "asia"},
    },
    {
        "id": "travel-4",
        "text": "New York City has iconic landmarks like the Statue of Liberty, Central Park, Times Square, and Broadway. Best visited in spring or fall. The subway runs 24/7. Budget $250-500/day for mid-range travel.",
        "metadata": {"source": "destination-guide", "topic": "nyc", "region": "north-america"},
    },
    {
        "id": "travel-5",
        "text": "For international flights, book 2-3 months in advance for best prices. Tuesday and Wednesday departures tend to be cheaper. Use layovers strategically to reduce costs. Always check visa requirements 60+ days before travel.",
        "metadata": {"source": "travel-tips", "topic": "flights", "region": "global"},
    },
    {
        "id": "travel-6",
        "text": "Travel insurance is recommended for international trips. It covers medical emergencies, trip cancellations, lost baggage, and flight delays. Annual policies are cost-effective for frequent travelers.",
        "metadata": {"source": "travel-tips", "topic": "insurance", "region": "global"},
    },
    {
        "id": "travel-7",
        "text": "Barcelona, Spain offers stunning Gaudí architecture, Mediterranean beaches, and vibrant nightlife. Best visited May-June or September-October. La Rambla, Sagrada Família, and Park Güell are must-sees. Budget $150-300/day.",
        "metadata": {"source": "destination-guide", "topic": "barcelona", "region": "europe"},
    },
    {
        "id": "travel-8",
        "text": "When packing for international travel, use packing cubes, bring a universal adapter, keep medications in carry-on, and always have copies of important documents. Roll clothes to save space.",
        "metadata": {"source": "travel-tips", "topic": "packing", "region": "global"},
    },
    {
        "id": "travel-9",
        "text": "Boutique hotels offer personalized service and unique character. Airbnb works well for longer stays and groups. Hostels are budget-friendly for solo travelers. Always read recent reviews and check cancellation policies.",
        "metadata": {"source": "travel-tips", "topic": "accommodation", "region": "global"},
    },
    {
        "id": "travel-10",
        "text": "Cape Town, South Africa offers Table Mountain, stunning coastline, wine country, and diverse wildlife. Best visited November-March (summer). Combine with a safari for the ultimate experience. Budget $100-250/day.",
        "metadata": {"source": "destination-guide", "topic": "cape-town", "region": "africa"},
    },
]


class RAGRetriever:
    """Vector-based retrieval using ChromaDB with OpenTelemetry instrumentation."""

    def __init__(self, app_metrics: AppMetrics):
        self.metrics = app_metrics
        self.collection_name = "knowledge_base"
        persist_dir = os.getenv("CHROMA_PERSIST_DIR", "./data/chroma_db")

        self.client = chromadb.Client(
            Settings(
                anonymized_telemetry=False,
                is_persistent=True,
                persist_directory=persist_dir,
            )
        )
        self._init_collection()

    def _init_collection(self):
        """Initialize the ChromaDB collection with sample documents."""
        with get_tracer().start_as_current_span("rag.init_collection") as span:
            span.set_attribute("rag.collection_name", self.collection_name)

            self.collection = self.client.get_or_create_collection(
                name=self.collection_name,
                metadata={"hnsw:space": "cosine"},
            )

            # Only add documents if collection is empty
            if self.collection.count() == 0:
                get_logger().info(
                    "rag.seeding_documents", count=len(SAMPLE_DOCUMENTS)
                )
                self.collection.add(
                    ids=[doc["id"] for doc in SAMPLE_DOCUMENTS],
                    documents=[doc["text"] for doc in SAMPLE_DOCUMENTS],
                    metadatas=[doc["metadata"] for doc in SAMPLE_DOCUMENTS],
                )
                span.set_attribute("rag.documents_seeded", len(SAMPLE_DOCUMENTS))
            else:
                span.set_attribute("rag.documents_existing", self.collection.count())

    def retrieve(self, query: str, n_results: int = 3) -> List[Dict[str, Any]]:
        """Retrieve relevant documents for a query with full tracing."""
        with get_tracer().start_as_current_span("rag.retrieve") as span:
            span.set_attribute("rag.query", query)
            span.set_attribute("rag.n_results", n_results)
            span.set_attribute("rag.collection", self.collection_name)

            start_time = time.time()
            try:
                results = self.collection.query(
                    query_texts=[query],
                    n_results=n_results,
                    include=["documents", "metadatas", "distances"],
                )

                duration = time.time() - start_time
                documents = []
                for i in range(len(results["ids"][0])):
                    documents.append(
                        {
                            "id": results["ids"][0][i],
                            "text": results["documents"][0][i],
                            "metadata": results["metadatas"][0][i],
                            "distance": results["distances"][0][i],
                        }
                    )

                span.set_attribute("rag.results_count", len(documents))
                span.set_attribute("rag.duration_ms", duration * 1000)
                span.set_status(StatusCode.OK)

                # Record metrics
                self.metrics.record_rag_retrieval(
                    duration=duration,
                    num_docs=len(documents),
                    collection=self.collection_name,
                )

                get_logger().info(
                    "rag.retrieval_complete",
                    query=query[:50],
                    results=len(documents),
                    duration_ms=round(duration * 1000, 2),
                )

                return documents

            except Exception as e:
                duration = time.time() - start_time
                span.set_status(StatusCode.ERROR, str(e))
                span.record_exception(e)
                get_logger().error("rag.retrieval_failed", error=str(e), query=query[:50])
                raise
