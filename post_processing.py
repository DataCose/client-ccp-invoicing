import json
from pathlib import Path

import pandas as pd

ZONES_JSON = None


def join_us_shipping_zones_from_shipstaton_export(
    shipment_export_csv: Path,
    zone_export_csv: Path,
    output_csv: Path,
    join_key_a="ship_id",
    join_key_b="Shipment ID",
    zone_column_name="Zone",
):
    shipments = pd.read_csv(shipment_export_csv)
    zones = pd.read_csv(zone_export_csv, encoding="ISO-8859-1")[
        [join_key_b, zone_column_name]
    ]

    merged = pd.merge(shipments, zones, left_on=join_key_a, right_on=join_key_b)
    merged.drop(join_key_b, axis=1, inplace=True)
    merged.rename(columns={"Zone": "zone"}, inplace=True)
    merged.to_csv(output_csv, index=False)


def clean_carriers(shipment_csv: Path, output_csv: Path):
    """
    Many UPS shipments have empty carrier fields. Check the service to see if it contains UPS and if so, set carrier.

    Also normalize stamps.com to USPS.
    """
    shipments = pd.read_csv(shipment_csv)
    shipments["service_lower"] = shipments["service"].str.lower()
    shipments.loc[
        (shipments["carrier"].isna() | (shipments["carrier"] == ""))
        & shipments["service_lower"].str.contains("ups"),
        "carrier",
    ] = "UPS"
    shipments.drop(columns="service_lower", inplace=True)
    shipments["carrier"] = shipments["carrier"].replace("Stamps.com", "USPS")
    shipments.to_csv(output_csv, index=False)


def zones_json() -> dict:
    global ZONES_JSON
    if not ZONES_JSON:
        with (
            Path(__file__).resolve().parent / "int_shipping_zones_country_codes.json"
        ).open() as file:
            ZONES_JSON = json.load(file)
    return ZONES_JSON


def find_zone(country, carrier):
    try:
        carrier_lower = carrier.lower()
        if carrier_lower in zones_json():
            for zone, countries in zones_json()[carrier_lower].items():
                if country in countries:
                    return zone
    except Exception as e:
        print(f"Country: {country}; Carrier: {carrier} \n\n Exception: {e}")
    return None


def set_international_shipping_zones(
    shipment_csv: Path,
    output_csv: Path,
    carrier_column_name="Carrier",
    country_column_name="Country",
):
    shipments = pd.read_csv(shipment_csv)
    shipments["zone"] = shipments.apply(
        lambda row: find_zone(row["country"], row["carrier"])
        if pd.isna(row["zone"]) or row["zone"] == ""
        else row["zone"],
        axis=1,
    )
    shipments.to_csv(output_csv, index=False)


if __name__ == "__main__":
    processed_path = (
        CURRENT_DIR := Path(__file__).resolve().parent
    ) / "clean_shipping_data.csv"
    join_us_shipping_zones_from_shipstaton_export(
        CURRENT_DIR / "out.csv", CURRENT_DIR / "zones.csv", processed_path
    )
    clean_carriers(processed_path, processed_path)
    set_international_shipping_zones(processed_path, processed_path)
