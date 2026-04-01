from omop_alchemy.cdm.model.structural import EpisodeView, Episode_Event
import sqlalchemy as sa

def test_episode_view_expected_domains():
    """Test episode view expected domains."""
    cls = EpisodeView

    assert "episode_concept_id" in cls.__expected_domains__
    assert "episode_object_concept_id" in cls.__expected_domains__
    assert "episode_type_concept_id" in cls.__expected_domains__

    assert cls.__expected_domains__["episode_concept_id"].domains == frozenset({"Episode"})


def test_episode_reference_context(session):
    """Test episode reference context."""
    ep = session.query(EpisodeView).first()
    assert ep is not None

    # ReferenceContext relationships
    assert ep.person is not None
    assert ep.episode_concept is not None
    assert ep.episode_type_concept is not None


def test_episode_has_episode_events(session):
    """Test episode has episode events."""
    ep = (
        session.query(EpisodeView)
        .filter(EpisodeView.episode_events.any())
        .first()
    )

    assert ep is not None
    assert len(ep.episode_events) > 0


def test_episode_event_resolves_target(session):
    """Test episode event resolves target."""
    ep = (
        session.query(EpisodeView)
        .filter(EpisodeView.episode_events.any())
        .first()
    )

    ee = ep.episode_events[0]
    target = ee.resolved_event

    assert target is not None
    # confirm that resolution gives us back a Condition_Occurrence/Drug_Exposure etc. object, not the original episode_event
    assert not isinstance(target, Episode_Event)
    assert hasattr(target, "__table__")
    assert hasattr(target, "person_id")

    assert target.__tablename__ == ee.event_table

    pk_cols = [c.name for c in sa.inspect(target.__class__).primary_key]
    assert len(pk_cols) == 1
    assert getattr(target, pk_cols[0]) == ee.event_id

    col = ee.resolved_event_id_column
    assert col is not None
    assert getattr(target, col) == ee.event_id



def test_episode_view_events_property(session):
    """Test episode view events property."""
    ep = (
        session.query(EpisodeView)
        .filter(EpisodeView.episode_events.any())
        .first()
    )

    events = ep.events

    assert isinstance(events, list)
    assert len(events) > 0

    for target in events:
        assert target is not None
        # confirm that resolution gives us back a Condition_Occurrence/Drug_Exposure etc. object, not the original episode_event
        assert not isinstance(target, Episode_Event)
        assert hasattr(target, "__table__")
        assert hasattr(target, "person_id")



def test_episode_parent_relationship(session):
    """Test episode parent relationship."""
    child = (
        session.query(EpisodeView)
        .filter(EpisodeView.episode_parent_id.isnot(None))
        .first()
    )

    if child:
        assert child.parent_episode is not None
        assert child.parent_episode.episode_id == child.episode_parent_id


def test_episode_date_bounds(session):
    """Test episode date bounds."""
    ep = session.query(EpisodeView).first()

    if ep.episode_end_date:
        assert ep.episode_start_date <= ep.episode_end_date
