import argparse
import csv
from pathlib import Path

from post_processing import find_zone


def update_zones(input_file: Path, output_file: Path = Path("out.csv")):
    with input_file.open() as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        for row in rows:
            country = row["COUNTRY"]
            if country == "US":
                continue
            carrier = row["CARRIER"]
            new_zone = find_zone(country=country, carrier=carrier)
            row["ZONE"] = new_zone

    with output_file.open("w") as f:
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()  # Başlık satırını yaz
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(
        description="Update ZONE codes according to FedEX guidelines."
    )
    parser.add_argument("--input", required=True, help="Input File in CSV format")
    parser.add_argument(
        "--out",
        help="Output csv file path (Optional - out.csv by default)",
        default="out.csv",
    )

    args = parser.parse_args()
    update_zones(Path(args.input), Path(args.out))


if __name__ == "__main__":
    # main()
    update_zones(Path("old_output.csv"))
