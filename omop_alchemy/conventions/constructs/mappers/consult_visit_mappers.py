import sqlalchemy as sa
import sqlalchemy.orm as so
from ..definitions.consult_visit_subqueries import specialist_trajectory, treatment_and_consult_windows, visits_by_specialty
from ....db import Base


class Specialist_Consult(Base):
    __table__ = specialist_trajectory
    person_id = so.column_property(__table__.c.person_id)
    __mapper_args__ = {
        "primary_key": [__table__.c.person_id]
    }
    first_specialist_consult = so.column_property(__table__.c.first_specialist_consult)
    last_specialist_consult = so.column_property(__table__.c.last_specialist_consult)
    initial_gp_referral = so.column_property(__table__.c.initial_gp_referral)
    first_pall_care_referral = so.column_property(__table__.c.first_pall_care_referral)
    first_specialist_care_visit = so.column_property(__table__.c.first_specialist_visit)
    first_pall_care_visit = so.column_property(__table__.c.first_pall_care_visit)



class Treatment_Consult_Window(Base):
    __table__ = treatment_and_consult_windows
    person_id = so.column_property(__table__.c.person_id)
    __mapper_args__ = {
        "primary_key": [__table__.c.person_id]
    }
    initial_gp_referral = so.column_property(__table__.c.initial_gp_referral)
    first_treatment = so.column_property(__table__.c.first_treatment)
    first_specialist = so.column_property(__table__.c.first_specialist)
    first_pall_care = so.column_property(__table__.c.first_pall_care)

    @sa.ext.hybrid.hybrid_property 
    def referral_to_specialist(self):
        if not(self.first_specialist) or not (self.initial_gp_referral):
            return None
        delta = self.first_specialist.date() - self.initial_gp_referral.date()
        return delta.days
    
    @referral_to_specialist.expression
    def referral_to_specialist(cls):
        return sa.cast(cls.first_specialist, sa.Date) - sa.cast(cls.initial_gp_referral, sa.Date)

    @sa.ext.hybrid.hybrid_property 
    def referral_to_tx(self):
        treat_starts = [d for d in [self.first_treatment, self.first_pall_care] if d is not None]
        if not(treat_starts):
            return None
        first_treatment = min(treat_starts)
        if not(first_treatment) or not (self.initial_gp_referral):
            return None
        delta = first_treatment.date() - self.initial_gp_referral.date()
        return delta.days
    
    @referral_to_tx.expression
    def referral_to_tx(cls):
        earliest_treatment_expr = sa.func.least(
            sa.case((cls.first_treatment != None, cls.first_treatment), else_=None),
            sa.case((cls.first_pall_care != None, cls.first_pall_care), else_=None)
        )
        return sa.cast(earliest_treatment_expr, sa.Date) - sa.cast(cls.initial_gp_referral, sa.Date)
                

class Visits_By_Specialty(Base):
    __table__ = visits_by_specialty
    
    person_id = visits_by_specialty.c.person_id
    visit_occurrence_id = visits_by_specialty.c.visit_occurrence_id
    visit_start_datetime = visits_by_specialty.c.visit_start_datetime
    provider_specialty = visits_by_specialty.c.provider_specialty
