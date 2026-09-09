"""Embedding-model migration: the vector equivalent of schema evolution.

Vectors from two embedding models are not comparable: a query embedded by
model B against an index of model-A vectors returns noise with a confident
score. So a model change is never an in-place update. The safe sequence:

1. **Shadow index.** Create ``<name>-<model>`` with the new model's dimension
   and a ``model`` tag; backfill it from the source of truth (the
   conversations), not from the old vectors.
2. **Dual write.** While the shadow catches up, every new conversation goes
   to both indexes (``DualWriter``).
3. **Compare.** Same labelled queries against both; the new index must match
   or beat the old on intent precision (``evaluate.evaluate``).
4. **Cut over.** Flip the pointer the API reads (``ActiveIndex``), a JSON
   document that names the live index and its model. Pinecone has no index
   aliases, so the pointer is ours.
5. **Retire.** Delete the old index once nothing reads it.

The pointer file is the only state outside Pinecone; in production it would
live in a config store (SSM / Dynamo), which is a one-function change.
"""

import json
import pathlib
from dataclasses import dataclass

from pinecone_deployment.conversations import Conversation
from pinecone_deployment.store import ConversationStore


@dataclass
class ActiveIndex:
    path: pathlib.Path

    def read(self) -> dict | None:
        if not self.path.exists():
            return None
        return json.loads(self.path.read_text())

    def write(self, index_name: str, model: str, dimension: int) -> None:
        self.path.write_text(
            json.dumps(
                {"index": index_name, "model": model, "dimension": dimension},
                indent=2,
            )
        )


class DualWriter:
    """Upsert into the live and the shadow store at once."""

    def __init__(self, live: ConversationStore, shadow: ConversationStore):
        self.live = live
        self.shadow = shadow

    def upsert(self, conversations: list[Conversation]) -> tuple[int, int]:
        return self.live.upsert(conversations), self.shadow.upsert(
            conversations
        )


def shadow_index_name(base: str, model: str) -> str:
    slug = "".join(ch if ch.isalnum() else "-" for ch in model.lower()).strip(
        "-"
    )
    return f"{base}-{slug}"[
        :45
    ]  # Pinecone index names: <= 45 chars, lowercase, dashes


def cut_over(pointer: ActiveIndex, shadow: ConversationStore) -> dict:
    pointer.write(
        shadow.index_name, shadow.embedder.model, shadow.embedder.dimension
    )
    return pointer.read()
