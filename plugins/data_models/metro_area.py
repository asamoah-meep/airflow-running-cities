from dataclasses import dataclass

@dataclass
class MetroArea:
    country: str
    state: str
    city: str
    latitude: float
    longitude: float
    park_accessibility: int
    park_density: int
    robbery_index: int

    @property
    def city_name(self):
        return self.city.lower().replace(' ', '_').replace(",", "").replace(".", "")