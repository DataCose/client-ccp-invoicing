from datacose.shipstation.models.generic import (
    Order,
    Shipment,
    ShipmentQuery,
    ShippingProvider,
    ShippingService
)


class ShipsationOrder(Order):
    ...


class ShipstationShipment(Shipment):
    ...


class ShipstationShipmentQuery(ShipmentQuery):
    ...


class ShipstationShippingProvider(ShippingProvider):
    ...


class ShipstationService(ShippingService):
    ...