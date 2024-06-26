import csv
import datetime
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from time import sleep
from typing import Dict, List, Optional

from platforms.shipstation.initializers import SHIPSTATION_ADMIN
from platforms.shipstation.models import (
    ShipmentQuery,
    ShipsationOrder,
    ShipstationService,
    ShipstationShipment,
    ShipstationShippingProvider,
)

logger = logging.getLogger("ExportShipstationShipments")
logging.basicConfig(
    filename="out.log", level=logging.INFO, filemode="w", encoding="utf8"
)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(console_handler)


@dataclass
class ShipmentCSVLine:
    print_date: datetime.date
    ship_id: int
    order_id: int
    items: str
    qty: int
    ship_date: datetime.date
    name: str
    id: Optional[str]
    phone: Optional[str]
    email: Optional[str]
    tracking: int
    carrier: str
    service: str
    country: str
    cost: float
    sku: str


class ExportShipstationShipments:
    def __init__(
        self,
        from_date: datetime.date,
        to_date: datetime.date,
        output_path: Optional[Path] = Path("out.csv"),
    ):
        self.from_date = from_date
        self.to_date = to_date
        self.shipstation_shipments: List[ShipstationShipment] = []
        self.csv_lines: List[ShipmentCSVLine] = []
        self.shipstation_order_map: Dict[str, ShipsationOrder] = {}
        self.shipstation_service_map: Dict[str, ShipstationService] = {}

        self.out = output_path

    def run(self):
        logger.info("Script started")
        self.get_shipments()
        self.get_carriers()
        self.get_services()
        self.get_orders()
        self.prepare_csv_lines()
        self.write_csv()

    def get_shipments(self):
        retry_count = 0
        while retry_count < 5:
            self.shipstation_shipments = ShipstationShipment.filter(
                initializer=SHIPSTATION_ADMIN,
                query=ShipmentQuery(
                    shipDateStart=self.from_date.isoformat(),
                    shipDateEnd=self.to_date.isoformat(),
                ),
            )
            if self.shipstation_shipments:
                break
            else:
                retry_count += 1
                logger.info(str(retry_count) + "th try to Get Shipments")

        logger.info(f"Successfully got {len(self.shipstation_shipments)} shipments")

    def get_carriers(self):
        carriers: List[
            ShipstationShippingProvider
        ] = ShipstationShippingProvider.filter(SHIPSTATION_ADMIN)
        self.carrier_code_carrier_map = {carrier.code: carrier for carrier in carriers}
        logger.info("Successfully got carriers")

    def get_services(self):
        for carrier_code in self.carrier_code_carrier_map.keys():
            for service in ShipstationService.filter(
                initializer=SHIPSTATION_ADMIN, carrier_code=carrier_code
            ):
                self.shipstation_service_map[service.code] = service
        logger.info("Successfully got carrier services")

    def get_orders(self):
        order_ids = {shipment.order_id for shipment in self.shipstation_shipments}

        orders = ShipsationOrder.filter_by_date(
            initializer=SHIPSTATION_ADMIN,
            start_date=self.from_date - datetime.timedelta(days=30),
            end_date=self.to_date,
        )

        for shipstation_order in orders:
            self.shipstation_order_map[shipstation_order.order_id] = shipstation_order

        for order_id in order_ids:
            if order_id in self.shipstation_order_map:
                continue

            retry_count = 0
            while retry_count < 5:
                try:
                    shipstation_order = ShipsationOrder.from_id(
                        order_id, initializer=SHIPSTATION_ADMIN
                    )
                    self.shipstation_order_map[
                        shipstation_order.order_id
                    ] = shipstation_order
                    break
                except:
                    sleep(2**retry_count)
                    retry_count += 1
            else:
                logger.warning("Skipped order: " + str(order_id))
        logger.info("Successfully got orders")

    def prepare_csv_lines(self):
        for shipment in self.shipstation_shipments:
            if shipment.voided:
                logger.warning("Shipment is voided " + str(shipment.shipment_id))
                continue
            order = self.shipstation_order_map.get(shipment.order_id)
            if not order:
                logger.warning("Shipment Order not found " + str(shipment.shipment_id))
                continue
            elif not shipment.shipment_items:
                logger.warning(
                    "Shipment items do not exist for Shipment "
                    + str(shipment.shipment_id)
                )
                continue
            for item in shipment.shipment_items:
                carrier_name = ""
                if carrier := self.carrier_code_carrier_map.get(shipment.carrier_code):
                    carrier_name = carrier.name

                service_name = ""
                if service := self.shipstation_service_map.get(shipment.service_code):
                    service_name = service.name
                csv_line = ShipmentCSVLine(
                    print_date=shipment.create_date,
                    ship_id=shipment.shipment_id,
                    order_id=shipment.order_id,
                    items=item.name,
                    qty=item.quantity,
                    ship_date=shipment.ship_date,
                    name=shipment.ship_to.name,
                    id=order.customer_id,
                    phone=shipment.ship_to.phone,
                    email=order.customer_email,
                    tracking=shipment.tracking_number,
                    carrier=carrier_name,
                    service=service_name,
                    country=shipment.ship_to.country,
                    cost=shipment.shipment_cost,
                    sku=item.sku,
                )
                self.csv_lines.append(csv_line)
        logger.info("Successfully prepared csv lines")

    def write_csv(self):
        if not self.csv_lines:
            logger.warning("Nothing to write")
            return

        with self.out.open("w", encoding="utf-8") as f:
            fieldnames = list(asdict(self.csv_lines[0]).keys())

            writer = csv.DictWriter(f, fieldnames=fieldnames)

            writer.writeheader()
            for csv_line in self.csv_lines:
                writer.writerow(asdict(csv_line))
        logger.info("Successfully created csv file")


def get_order(order_id):
    order = ShipsationOrder.from_id(initializer=SHIPSTATION_ADMIN, _id=order_id)
    return order


def get_services():
    # shipment = ShipstationShipment.from_id(initializer=SHIPSTATION_ADMIN, _id="220275386")
    # carriers = ShipstationShippingProvider(initialer=SHIPSTATION_ADMIN, )
    services = ShipstationService.filter(
        initializer=SHIPSTATION_ADMIN, carrier_code="fedex"
    )
    return services


if __name__ == "__main__":
    ExportShipstationShipments(
        datetime.date(2024, 3, 1),
        datetime.date(2024, 3, 31),
        Path("tr_out.csv"),
    ).run()
