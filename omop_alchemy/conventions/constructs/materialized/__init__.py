from .MaterializedViewMixin import MaterializedViewMixin
from .ModifiedProcedure import ModifiedProcedure
from .OverarchingEpisode import OverarchingCondition
from .ConditionTreatmentEpisode import ConditionTreatmentEpisode
from .RadiotherapyEpisode import RTCourse, Fraction
from .SACTEpisode import SACTRegimen, Cycle
from .Surgeries import Radioisotope, SurgicalProcedure
from .ModifiedCondition import LatModifier, StageModifier, GradeModifier, SizeModifier, ModifiedCondition
from .TreatmentEnvelope import TreatmentEnvelope
from .SpecialistVisits import Specialist_Consult, VisitsBySpecialty, ConsultWindow

def create_mat_views(session):
    for view in [
        LatModifier, StageModifier, GradeModifier, SizeModifier, ModifiedCondition, ModifiedProcedure, 
        OverarchingCondition, Cycle, Fraction, SACTRegimen, RTCourse, Radioisotope, SurgicalProcedure, 
        ConditionTreatmentEpisode, TreatmentEnvelope, Specialist_Consult, VisitsBySpecialty, ConsultWindow
    ]:
        view.create_mv(session)
