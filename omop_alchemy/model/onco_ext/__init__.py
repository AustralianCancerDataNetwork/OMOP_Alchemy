from .episode import Episode
from .episode_event import Episode_Event

for concept_linked_table in [Episode]:
    concept_linked_table.add_concepts()

__all__ = [Episode, Episode_Event]