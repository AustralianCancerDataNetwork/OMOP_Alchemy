from ..definitions.modifier_subqueries import T_Stage, N_Stage, M_Stage, Group_Stage, Grade, Laterality, cancer_dx_join, path_stage_join, episode_stage_join
import sqlalchemy as sa
import sqlalchemy.orm as so
from ....db import Base

class Cancer(Base):
    __table__ = cancer_dx_join
    person_id = so.column_property(__table__.c.person_id)
    cancer_diagnosis_id = so.column_property(__table__.c.cancer_diagnosis_id)
    cancer_start_date = so.column_property(__table__.c.cancer_start_date)
    t_stage_value = so.column_property(__table__.c.t_stage_value)
    t_stage_date = so.column_property(__table__.c.t_stage_date)
    n_stage_value = so.column_property(__table__.c.n_stage_value)
    n_stage_date = so.column_property(__table__.c.n_stage_date)
    m_stage_value = so.column_property(__table__.c.m_stage_value)
    m_stage_date = so.column_property(__table__.c.m_stage_date)
    group_stage_value = so.column_property(__table__.c.group_stage_value)
    group_stage_date = so.column_property(__table__.c.group_stage_date)
    grade_value = so.column_property(__table__.c.grade_value)
    grade_date = so.column_property(__table__.c.grade_date)
    laterality_value = so.column_property(__table__.c.laterality_value)
    laterality_date = so.column_property(__table__.c.laterality_date)


class DxPathStage(Base):
    __table__ = path_stage_join
    person_id = so.column_property(__table__.c.person_id)
    cancer_diagnosis_id = so.column_property(__table__.c.cancer_diagnosis_id)
    cancer_start_date = so.column_property(__table__.c.cancer_start_date)
    path_stage_date = so.column_property(__table__.c.path_stage_date)


class EpisodeStage(Base):
    __table__ = episode_stage_join
    
    person_id = so.column_property(__table__.c.person_id)
    episode_id = so.column_property(__table__.c.episode_id)
    episode_start_datetime = so.column_property(__table__.c.episode_start_datetime)
    stage_concept = so.column_property(__table__.c.measurement_concept_id)
    stage_date = so.column_property(__table__.c.measurement_date)