from .MaterializedViewMixin import MaterializedViewMixin
from .ModifiedProcedure import ModifiedProcedure
from .OverarchingEpisode import OverarchingCondition
from .ConditionTreatmentEpisode import ConditionTreatmentEpisode
from .RadiotherapyEpisode import RTCourse, Fraction
from .SACTEpisode import SACTRegimen, Cycle
from .Surgeries import Radioisotope, SurgicalProcedure
from .ModifiedCondition import LatModifier, StageModifier, GradeModifier, SizeModifier, ModifiedCondition, TStage, NStage, MStage, GroupStage, MetsModifier
from .TreatmentEnvelope import TreatmentEnvelope
from .SpecialistVisits import SpecialistConsult, ConsultWindow, VisitsBySpecialty, DXRelevantVisit
from .PersonDemography import PersonDemography
from .ForceDxLink import FEVMeas, PYHMeas, DistressThermMeas, BSAMeas, Height, WeightChange, Weight, EstGFRate, CreatClearance, GenomicVariants, DXRelevantProc, DXRelevantObs, DXRelevantMeas

def create_mat_views(session):
    for view in [
    LatModifier, StageModifier, TStage, NStage, MStage, GroupStage, GradeModifier, 
    SizeModifier, ModifiedCondition, ModifiedProcedure, PersonDemography, MetsModifier,
    OverarchingCondition, Cycle, Fraction, SACTRegimen, RTCourse, Radioisotope, SurgicalProcedure, 
    ConditionTreatmentEpisode, TreatmentEnvelope, FEVMeas, PYHMeas, DistressThermMeas, BSAMeas, 
    Height, WeightChange, Weight, EstGFRate, CreatClearance, GenomicVariants, 
    DXRelevantProc, DXRelevantObs, DXRelevantVisit, DXRelevantMeas, SpecialistConsult, VisitsBySpecialty, ConsultWindow,
]:
        view.create_mv(session)
