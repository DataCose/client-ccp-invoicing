import json
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


def join_us_shipping_zones_from_shipstaton_export(
    shipment_export_csv: Path,
    zone_export_csv: Path,
    output_csv: Path,
    join_key_a="ship_id",
    join_key_b="Shipment ID",
    zone_column_name="Zone",
):
    shipments = pd.read_csv(shipment_export_csv)
    shipments.drop(["zone", "description"], axis=1, errors="ignore", inplace=True)
    zones = pd.read_csv(zone_export_csv, encoding="ISO-8859-1")[
        [join_key_b, zone_column_name]
    ]

    merged = pd.merge(
        shipments, zones, how="left", left_on=join_key_a, right_on=join_key_b
    )
    merged.drop(join_key_b, axis=1, inplace=True)
    merged.rename(columns={zone_column_name: zone_column_name.lower()}, inplace=True)
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


def set_international_shipping_zones(
    shipment_csv: Path,
    output_csv: Path,
    carrier_column_name="Carrier",
    country_column_name="Country",
):
    def find_zone(country, carrier):
        try:
            carrier_lower = carrier.lower()
            if carrier_lower in zones_json:
                for zone, countries in zones_json[carrier_lower].items():
                    if country in countries:
                        return zone
        except Exception as e:
            print(f"Country: {country}; Carrier: {carrier} \n\n Exception: {e}")
        return None

    shipments = pd.read_csv(shipment_csv)

    with (
        Path(__file__).resolve().parent / "int_shipping_zones_country_codes.json"
    ).open() as file:
        zones_json = json.load(file)

    shipments["zone"] = shipments.apply(
        lambda row: find_zone(row["country"], row["carrier"])
        if pd.isna(row["zone"]) or row["zone"] == ""
        else row["zone"],
        axis=1,
    )
    shipments.to_csv(output_csv, index=False)


def join_project_name_from_mapper(
    shipment_csv: Path,
    mapper_csv: Path,
    output_csv: Path,
    join_key_a="sku",
    join_key_b="SKU",
    project_name_column_name="Project Name",
):
    shipments = pd.read_csv(shipment_csv)
    project_mapper = pd.read_csv(mapper_csv)[[join_key_b, project_name_column_name]]

    merged = pd.merge(
        shipments, project_mapper, how="left", left_on=join_key_a, right_on=join_key_b
    )
    merged.drop(join_key_b, axis=1, inplace=True)
    merged.rename(
        columns={project_name_column_name: project_name_column_name.lower()},
        inplace=True,
    )
    merged.to_csv(output_csv, index=False)


def final_clean_for_google_sheet(
    shipment_csv: Path, output_csv: Path, project_name: Optional[str]
):
    shipments = pd.read_csv(shipment_csv)
    if project_name:
        shipments = shipments[shipments["project name"] == project_name]
    shipments.drop(
        ["order_id", "sku", "project name"], axis=1, errors="ignore", inplace=True
    )
    shipments["print_date"] = pd.to_datetime(shipments["print_date"]).dt.date
    shipments.to_csv(output_csv, encoding="utf-8", index=False)


def join_rerated_ups_shipping_amounts(
    shipment_csv: Path,
    rerated_csv: Path,
    output_csv: Path,
    join_key_a="tracking",
    join_key_b="TRACKING_2",
    new_cost_column="UPS COST",
):
    shipments = pd.read_csv(shipment_csv)
    rerated_shipments = pd.read_csv(rerated_csv, encoding="ISO-8859-1")[
        [join_key_b, new_cost_column]
    ]

    merged = pd.merge(
        shipments,
        rerated_shipments,
        how="left",
        left_on=join_key_a,
        right_on=join_key_b,
    )
    merged["cost"] = np.where(
        merged[new_cost_column].notna(), merged[new_cost_column], merged["cost"]
    )
    merged.drop(columns=[new_cost_column, join_key_b], axis=1, inplace=True)
    merged.rename(columns={new_cost_column: new_cost_column.lower()}, inplace=True)
    merged.to_csv(output_csv, index=False)


def main(
    client_name: str, project_name: Optional[str], raw_shipstation_export_file: str
):
    # Processing
    raw_shipstation_export = (
        current_dir := Path(__file__).resolve().parent
    ) / raw_shipstation_export_file
    processed_path = current_dir / "clean_shipping_data.csv"
    join_us_shipping_zones_from_shipstaton_export(
        raw_shipstation_export, current_dir / "zones.csv", processed_path
    )
    clean_carriers(processed_path, processed_path)
    set_international_shipping_zones(processed_path, processed_path)
    if project_name:
        join_project_name_from_mapper(
            processed_path,
            current_dir / "mappers" / f"{client_name.lower()}.csv",
            processed_path,
        )

    if not (
        final_output_client_directory := current_dir / client_name.replace(" ", "_")
    ).is_dir():
        final_output_client_directory.mkdir()

    final_output_file_path = (
        final_output_client_directory
        / f"{(project_name or client_name).replace(' ', '_')}.csv"
    )
    final_clean_for_google_sheet(processed_path, final_output_file_path, project_name)
    # join_rerated_ups_shipping_amounts(final_output_file_path, CURRENT_DIR / 'dg_ups_rerated.csv',
    #                                   final_output_file_path)


if __name__ == "__main__":
    client_projects = {
        "TonyRobbins": {
            "raw_shipstation_export_file": "tr_out.csv",
            "projects": [
                "TR UPW MARCH 2024",
                "TR Employee Welcome Kit",
                "2024 TR Contractor Boxes",
                "Life Mastery Virtual Sept 2023 (workbook only)",
                "TR PLATINUM BIRTHDAYS",
                "TR BM1 JAN 2024",
            ],
        },
        "WakeUpWarrior": {
            "raw_shipstation_export_file": "warrior_out.csv",
            "projects": [None],
        },
        "ClickFunnels": {
            "raw_shipstation_export_file": "cf_out.csv",
            "projects": [None],
        },
        "GeneralAccount": {
            "raw_shipstation_export_file": "ga_out.csv",
            "projects": [None],
        },
        "DG": {
            "raw_shipstation_export_file": "dg_out.csv",
            "projects": [None],
        },
    }
    CLIENT_NAME = "DG"
    for project in (client_data := client_projects.get(CLIENT_NAME, {})).get(
        "projects", []
    ):
        main(
            client_name=CLIENT_NAME,
            project_name=project,
            raw_shipstation_export_file=client_data.get("raw_shipstation_export_file"),
        )
