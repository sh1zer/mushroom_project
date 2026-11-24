import io
import sys
import urllib.request
import zipfile
from pathlib import Path

URL = "https://archive.ics.uci.edu/static/public/848/secondary+mushroom+dataset.zip"
RAW = Path(__file__).resolve().parents[1] / "data" / "raw"
WANTED = {"secondary_data.csv", "secondary_data_meta.txt"}


def main() -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    print(f"downloading {URL}")
    with urllib.request.urlopen(URL, timeout=60) as r:
        outer = zipfile.ZipFile(io.BytesIO(r.read()))
    inner = zipfile.ZipFile(io.BytesIO(outer.read("MushroomDataset.zip")))
    for name in inner.namelist():
        base = name.split("/")[-1]
        if base in WANTED:
            (RAW / base).write_bytes(inner.read(name))
            print(f"saved {RAW / base}")
    missing = WANTED - {p.name for p in RAW.iterdir()}
    if missing:
        sys.exit(f"missing files: {missing}")


if __name__ == "__main__":
    main()
