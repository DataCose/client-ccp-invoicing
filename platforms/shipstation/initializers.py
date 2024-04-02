from datacose.shipstation.client import ShipStation
from datacose.shipstation.initializer import ShipStationModelInitializer

SHIPSTATION_ADMIN = ShipStationModelInitializer(
    sdk=ShipStation(
        "d73a353ab1a94f50a3b74acce14d499e", "e11ecf2cd9644cfd81d7d2cceaee65ae"
    )
)
