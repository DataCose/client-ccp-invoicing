from datacose.shipstation.client import ShipStation
from datacose.shipstation.initializer import ShipStationModelInitializer

SHIPSTATION_ADMIN = ShipStationModelInitializer(
    sdk=ShipStation(
        "fef3419b2c564394bc4d4462d25edcbb", "b8c2f550bf014a529edb63c9f6316449"
    )
)
