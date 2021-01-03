from datetime import datetime, timedelta

EPOCH = datetime(year=1970, month=1, day=1)


# Source: https://github.com/volkszaehler/volkszaehler.org/blob/master/lib/Definition/EntityDefinition.json
CHANNEL_TYPES = [
    {
        "name": "group",
        "translation": {"de": "Gruppe", "en": "Group", "fr": "Groupe"}
    },
    {
        "name": "building",
        "translation": {"de": "Gebäude", "en": "Building", "fr": "Immeuble"}
    },
    {
        "name": "user",
        "translation": {"de": "Nutzer", "en": "User", "fr": "Usager"}
    },
    {
        "name": "power",
        "required": ["resolution"],
        "unit": "kWh",
        "hasConsumption": True,
        "translation": {
            "de": "El. Energie (S0-Impulse)",
            "en": "El. Energy (s0-pulses)"
        }
    },
    {
        "name": "powersensor",
        "optional": ["resolution"],
        "unit": "kW",
        "hasConsumption": True,
        "translation": {
            "de": "El. Energie (Leistungswerte)",
            "en": "El. Energy (power readings)"
        }
    },
    {
        "name": "electric meter",
        "required": ["resolution"],
        "unit": "kWh",
        "hasConsumption": True,
        "translation": {
            "de": "El. Energie (Zählerstände)",
            "en": "El. Energy (absolute meter readings)"
        }
    },
    {
        "name": "voltage",
        "optional": ["resolution"],
        "unit": "V",
        "translation": {
            "de": "Spannungssensor",
            "en": "Voltage Meter",
            "fr": "Voltmètre"
        }
    },
    {
        "name": "current",
        "optional": ["resolution"],
        "unit": "A",
        "translation": {
            "de": "Stromsensor",
            "en": "Current Meter",
            "fr": "Courantmètre"
        }
    },
    {
        "name": "gas",
        "required": ["resolution"],
        "unit": "m³/h",
        "hasConsumption": True,
        "translation": {
            "de": "Gas (S0-Impulse)",
            "en": "Gas (S0-pulses)",
            "fr": "Gaz (S0)"
        }
    },
    {
        "name": "gas sensor",
        "required": ["resolution"],
        "unit": "m³/h",
        "hasConsumption": True,
        "translation": {
            "de": "Gas (Durchflusswerte)",
            "en": "Gas (flow rate)",
            "fr": "Gaz (vitesse d'écoulement)"
        }
    },
    {
        "name": "gas meter",
        "required": ["resolution"],
        "unit": "m³",
        "hasConsumption": True,
        "translation": {"de": "Gas (Zählerstände)", "en": "Gas (meter readings)"}
    },
    {
        "name": "heat",
        "required": ["resolution"],
        "unit": "kWh",
        "hasConsumption": True,
        "translation": {
            "de": "Wärme (S0-Impulse)",
            "en": "Heat (S0-pulses)",
            "fr": "Énergie thermique (S0)"
        }
    },
    {
        "name": "heatsensor",
        "optional": ["resolution"],
        "unit": "kW",
        "hasConsumption": True,
        "translation": {
            "de": "Wärme (Leistungswerte)",
            "en": "Heat (power readings)",
            "fr": "Énergie thermique (puissance)"
        }
    },
    {
        "name": "heattotal",
        "required": ["resolution"],
        "unit": "kWh",
        "hasConsumption": True,
        "translation": {
            "de": "Wärme (Zählerstände)",
            "en": "Heat (absolute meter readings)",
            "fr": "Énergie thermique (consommation compteur)"
        }
    },
    {
        "name": "temperature",
        "optional": ["resolution"],
        "unit": "°C",
        "translation": {
            "de": "Temperatur",
            "en": "Temperature",
            "fr": "Température"
        }
    },
    {
        "name": "water",
        "required": ["resolution"],
        "unit": "l/h",
        "hasConsumption": True,
        "translation": {
            "de": "Wasser (S0-Impulse)",
            "en": "Water (S0-pulses)",
            "fr": "Eau (S0)"
        }
    },
    {
        "name": "flow",
        "optional": ["resolution"],
        "unit": "m³/h",
        "hasConsumption": True,
        "translation": {
            "de": "Wasser (Durchflusswerte)",
            "en": "Water (flow rate readings)",
            "fr": "Eau (vitesse d'écoulement)"
        }
    },
    {
        "name": "watertotal",
        "required": ["resolution"],
        "unit": "l/h",
        "hasConsumption": True,
        "translation": {
            "de": "Wasser (Zählerstände)",
            "en": "Water (absolute meter readings)",
            "fr": "Eau (consommation compteur)"
        }
    },
    {
        "name": "filllevel",
        "optional": ["resolution", "tolerance", "cost", "local"],
        "unit": "l",
        "translation": {
            "de": "Füllstand",
            "en": "Fill Level",
            "fr": "Niveau de remplissage"
        }
    },
    {
        "name": "workinghours",
        "required": ["resolution"],
        "optional": ["tolerance", "local", "gap"],
        "unit": "h",
        "hasConsumption": True,
        "translation": {
            "de": "Betriebsstundenzähler (Impulse)",
            "en": "Operation Hours Meter (Impulses)",
            "fr": "Compteur horaire"
        }
    },
    {
        "name": "workinghourstotal",
        "required": ["resolution"],
        "optional": ["tolerance", "local", "gap"],
        "unit": "h",
        "hasConsumption": True,
        "translation": {
            "de": "Betriebsstundenzähler (Zählerstand)",
            "en": "Operation Hours Meter (meter readings)",
            "fr": "Compteur horaire"
        }
    },
    {
        "name": "workinghourssensor",
        "optional": ["resolution"],
        "unit": "h",
        "hasConsumption": True,
        "translation": {
            "de": "Betriebsstundensensor",
            "en": "Operating Hours Sensor"
        }
    },
    {
        "name": "valve",
        "optional": ["resolution"],
        "unit": "%",
        "translation": {"de": "Ventil", "en": "valve", "fr": "valve"}
    },
    {
        "name": "pressure",
        "optional": ["resolution"],
        "unit": "hPa",
        "translation": {
            "de": "Luftdruck",
            "en": "Barometric Pressure",
            "fr": "Pression d'air"
        }
    },
    {
        "name": "humidity",
        "optional": ["resolution"],
        "unit": "%",
        "translation": {
            "de": "Luftfeuchtigkeit",
            "en": "Air Humidity",
            "fr": "Hygrométrie"
        }
    },
    {
        "name": "humidity absolute",
        "optional": ["resolution"],
        "unit": "g/m³",
        "translation": {
            "de": "absolute Luftfeuchtigkeit",
            "en": "absolute humidity",
            "fr": "Humidité absolue"
        }
    },
    {
        "name": "windspeed",
        "optional": ["resolution"],
        "unit": "km/h",
        "translation": {
            "de": "Windgeschwindigkeit",
            "en": "Windspeed",
            "fr": "Vitesse du vent"
        }
    },
    {
        "name": "fanspeed",
        "optional": ["resolution"],
        "unit": "u/min",
        "translation": {
            "de": "Drehzahl",
            "en": "Fan speed"
        }
    },
    {
        "name": "luminosity",
        "optional": ["resolution"],
        "unit": "cd",
        "translation": {
            "de": "Lichtstärke",
            "en": "Luminosity",
            "fr": "Intensité lumineuse"
        }
    },
    {
        "name": "illumination",
        "optional": ["resolution"],
        "unit": "lx",
        "translation": {
            "de": "Beleuchtungsstärke",
            "en": "Illumination"
        }
    },
    {
        "name": "frequency",
        "optional": ["resolution"],
        "unit": "Hz",
        "translation": {
            "de": "Frequenz",
            "en": "Frequency"
        }
    },
    {
        "name": "universalsensor",
        "required": ["unit"],
        "optional": ["resolution"],
        "translation": {"de": "Sensor", "en": "Sensor"}
    },
    {
        "name": "consumptionsensor",
        "required": ["unit"],
        "optional": ["resolution"],
        "hasConsumption": True,
        "translation": {"de": "Verbrauchssensor", "en": "Consumption Sensor"}
    },
    {
        "name": "co2 concentration",
        "optional": [],
        "icon": "propeller.png",
        "unit": "ppm",
        "translation": {
            "de": "CO2-Konzentration",
            "en": "CO2 concentration",
            "fr": "Concentration CO2"
        }
    }
]


def time(t):
    """Convert timestamp to datetime object

    Arguments:
        t (int): Timestamp [ms since EPOCH]

    Return:
        datetime.datetime: Datetime object representing the same time as `t`
    """
    return EPOCH + timedelta(milliseconds=t)


def timestamp(t):
    """Convert datetime object to timestamp

    Arguments:
        t (datetime.datetime): Datetime object

    Return:
        int: Timestamp [ms since EPOCH]
    """
    return int(1000. * (t - EPOCH).total_seconds() + 0.5)


def now():
    """Get current UTC time as timestamp

    Return:
        int: Timestamp [ms since EPOCH]
    """
    return timestamp(datetime.utcnow())
