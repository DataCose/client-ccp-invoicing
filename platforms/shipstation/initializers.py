from datacose.shipstation.client import ShipStation
from datacose.shipstation.initializer import ShipStationModelInitializer

SHIPSTATION_ADMIN = ShipStationModelInitializer(sdk=ShipStation("***", "***"))
