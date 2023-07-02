import pytest
from unittest.mock import Mock

from data_models import MetroArea

class TestMetroArea:

    @pytest.mark.parametrize("city_name, expected", [
        ("capitalCity", "capitalcity"),
        ("city. with. punctuation", "city_with_punctuation")
    ])
    def test_city_name(self, city_name, expected):
        metro_area = MetroArea(Mock(),Mock(),city_name, Mock(), Mock(), Mock(), Mock(), Mock())
        assert metro_area.city_name == expected