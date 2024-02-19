from .episode import Episode

for concept_linked_table in [Episode]:
    concept_linked_table.add_concepts()

__all__ = [Episode]