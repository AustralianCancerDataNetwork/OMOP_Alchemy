from typing import Callable
import sqlalchemy as sa
import sqlalchemy.orm as so

from .vocab_handlers import ConceptResolver

class ConceptResolverRegistry:
    """
    Lazy registry for ConceptResolvers.

    Resolvers are constructed on first access and cached for the lifetime
    of this registry instance. The registry is scoped to a SQLAlchemy Engine,
    ensuring vocab lookups are built once per database.
    """

    def __init__(self, engine: sa.Engine):
        self.engine = engine
        self._cache: dict[str, ConceptResolver] = {}
        self._builders: dict[str, Callable[[so.Session], ConceptResolver]] = {}

    
    def register(self, name: str, builder: Callable[[so.Session], ConceptResolver]) -> None:
        """
        Register a named resolver builder.

        This does not construct the resolver immediately; it only records
        how to build it when first requested.
        """
        if name in self._builders:
            raise KeyError(f"Resolver '{name}' is already registered")

        self._builders[name] = builder

    def get(self, name: str) -> ConceptResolver:
        """
        Return a cached resolver by name, building it lazily if required.

        The resolver must have been registered via ``register``.
        """
        if name in self._cache:
            return self._cache[name]

        if name not in self._builders:
            raise KeyError(
                f"No resolver named '{name}' is registered. "
                f"Available resolvers: {sorted(self._builders)}"
            )

        builder = self._builders[name]

        with so.Session(self.engine) as session:
            resolver = builder(session)

        self._cache[name] = resolver
        return resolver

    def __getitem__(self, name: str) -> ConceptResolver:
        return self.get(name)

    def __contains__(self, name: str) -> bool:
        return name in self._builders