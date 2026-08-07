"""Construct episodes and resolve the relationships between them.

Episodes in the CDM are rows that reference each other through
``episode_parent_id`` and reach clinical facts through ``Episode_Event``.
Turning that into something queryable — a regimen with its cycles, a
diagnosis with the treatment that followed — is what this module does.

It provides queries that select episodes by concept, that join parent and
child episodes into a single result so a hierarchy can be read in one
pass, and that establish the date windows relating one episode to
another.

These queries are written against the raw ``Episode`` and
``Episode_Event`` tables and depend on no materialised views, so they run
against any conformant CDM instance.  Query construction against
materialised-view subclasses lives in ``omop-constructs``.

Callers supply the episode concept IDs they care about.  Derivation
determines structure; it does not decide which concepts constitute a
regimen or a disease course for a given specialty.
"""
