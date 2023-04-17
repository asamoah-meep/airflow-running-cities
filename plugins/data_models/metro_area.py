from dataclasses import dataclass

@dataclass
class MetroArea:
    country: str
    state: str
    city: str
    latitude: float
    longitude: float

    @property
    def city_name(self):
        return self.city.lower().replace(' ', '_')