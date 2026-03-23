from omop_alchemy.maintenance.ascii import BANNERS, banner_for_width, render_banner


def test_banner_for_width_uses_big_banner_when_space_allows():
    banner = banner_for_width(BANNERS["big"].width + 4)

    assert banner.name == "big"


def test_banner_for_width_falls_back_to_small_banner():
    banner = banner_for_width(BANNERS["big"].width - 1)

    assert banner.name == "small"


def test_render_banner_returns_banner_text():
    rendered = render_banner(BANNERS["small"].width)

    assert "▗▄▖" in rendered
