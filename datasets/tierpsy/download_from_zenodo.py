from sickle import Sickle
import re
import csv
import os
import shutil
import urllib.parse, urllib.request
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

URL = "https://zenodo.org/oai2d"
DATASET = "user-open-worm-movement-database"


def download_records(input_file: str):
    with open(input_file, "r") as f:
        reader = csv.reader(f, delimiter=",")
        lines = [line[0] for line in reader]

    out_dir = lines[0]
    names = lines[1:]

    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    os.mkdir(out_dir)

    sickle = Sickle(URL)
    recs = sickle.ListRecords(metadataPrefix="oai_dc", set=DATASET, ignore_deleted=True)

    infos = []
    for rec in recs:
        metadata = rec.metadata

        desc = metadata["description"][0]
        find_base_name = re.search("base_name : (.*)\n", desc)
        if find_base_name is not None:
            name = find_base_name.group(1)
            if name in names:
                identifier = metadata["identifier"][0]
                infos.append((name, identifier))

    for name, identifier in infos:

        record_path = os.path.join(out_dir, name)
        if not os.path.exists(record_path):
            os.mkdir(record_path)

        filename = name + ".hdf5"
        features_filename = name + "_features.hdf5"
        wcon_filename = name + ".wcon.zip"

        if not os.path.exists(os.path.join(record_path, filename)):
            url = identifier + "/files/" + urllib.parse.quote(filename)
            urllib.request.urlretrieve(url, os.path.join(record_path, filename))

            url = identifier + "/files/" + urllib.parse.quote(features_filename)
            urllib.request.urlretrieve(url, os.path.join(record_path, features_filename))

            url = identifier + "/files/" + urllib.parse.quote(wcon_filename)
            urllib.request.urlretrieve(url, os.path.join(record_path, wcon_filename))

            logger.info(f"Downloaded {name}")


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input_file",
        type=str,
        help="Path to csv file containing the names of the records"
        " to download from the Open Worm Database on Zenodo",
    )
    args = parser.parse_args()
    download_records(**vars(args))


if __name__ == "__main__":
    main()
