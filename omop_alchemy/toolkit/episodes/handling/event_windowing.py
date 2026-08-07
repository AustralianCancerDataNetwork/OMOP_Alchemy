from __future__ import annotations

from datetime import date, timedelta

DEFAULT_EPISODE_WINDOW_DAYS_PRIOR = 90
DEFAULT_EPISODE_OPEN_END_FALLBACK_DAYS = 365


def episode_attachment_window(
    episode,
    *,
    days_prior: int = DEFAULT_EPISODE_WINDOW_DAYS_PRIOR,
    open_end_fallback_days: int = DEFAULT_EPISODE_OPEN_END_FALLBACK_DAYS,
) -> tuple[date, date]:
    """
    Date window used for episode-attributable facts admitted by date rather
    than by an explicit ``Episode_Event`` link.

    Time-varying facts such as body measurements use a bounded
    episode-relative window. Open episodes fall back to a finite post-start
    window so an accidentally long-running episode does not absorb a
    person's entire future history.
    """
    start = episode.episode_start_date
    end = episode.episode_end_date

    window_start = start - timedelta(days=days_prior)
    open_end_bound = start + timedelta(days=open_end_fallback_days)
    window_end = min(end, open_end_bound) if end is not None else open_end_bound

    return window_start, window_end
