# we have to import at least one model from each module to ensure they are registered
from .clinical import Person
from .vocabulary import Concept
from .health_system import Visit_Occurrence
from .health_economic import Cost
from .structural import Episode
from .unstructured import Note

__all__ = [
    "Person",
    "Concept",       
    "Visit_Occurrence",
    "Cost",
    "Episode",
    "Note",
]