import enum

class ConceptEnum(enum.Enum):

    @classmethod
    def member_values(cls):
        return (s.value for s in cls)
    
    @classmethod
    def is_member(cls, val):
        return not val or val in [s.value for s in cls]

    @classmethod
    def labels(cls):
        return [s.name for s in cls]
    
class ModifierFields(ConceptEnum):
    condition_occurrence_id = 1147127
    drug_exposure_id = 1147707
    procedure_occurrence_id = 1147082

class ModifierTables(ConceptEnum):
    drug_exposure = 1147339
    episode = 35225440
    observation = 1147304

class TreatmentEpisode(ConceptEnum):
    care_plan_assignment = 4207655  # SNOMED - Prescription of therapeutic regimen
    ehr_prescription = 32838        # EHR prescription
    ehr_planned_dispensing = 32837  # EHR planned dispensation
    ehr_encounter_record = 32827    # EHR encounter
    
class Modality(ConceptEnum):
    chemotherapy = 35803401
    radiotherapy = 35803411
    
class EpisodeConcepts(ConceptEnum):   
    episode_of_care = 32533           # Overarching disease episode
    treatment_regimen = 32531         # Assignment to or derivation of treatment regimen   
    treatment_cycle = 32532           # Assignment to or derivation of treatment cycle
    disease_first_occurrence = 32528  # Initial diagnosis
    disease_progression = 32677       # Diagnosis that is linked to another primary

class EpisodeTypes(ConceptEnum):
    ehr_defined = 32544               # Episode defined in EHR
    ehr_derived = 32545               # Episode derived algorithmically from EHR
    
class ConditionModifiers(ConceptEnum):
    # for measurement_concept_id grouping
    init_diag = 734306                # Cancer Modifier - Initial Diagnosis
    