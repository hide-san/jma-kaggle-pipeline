"""
scripts/create_notebook_jma_datasets_overview.py
=================================================
Generate and publish the JMA Datasets Overview notebook to Kaggle.

The notebook renders the project README live from GitHub and shows a table
of all 33 JMA datasets managed by this pipeline with clickable Kaggle links.

Output
------
notebooks/jma_datasets_overview/
    jma_datasets_overview.ipynb      nbformat v4 notebook
    kernel-metadata.json             Kaggle Kernels API descriptor

Usage
-----
    python scripts/create_notebook_jma_datasets_overview.py

Requires KAGGLE_USERNAME and KAGGLE_API_TOKEN environment variables.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

# Allow importing from project root when run as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config  # noqa: E402 — triggers DATASET_REGISTRY population

from jma_api_client.base import DATASET_REGISTRY  # noqa: E402

GITHUB_REPO_URL = "https://github.com/hide-san/jma-kaggle-pipeline"
GITHUB_RAW_BASE = "https://raw.githubusercontent.com/hide-san/jma-kaggle-pipeline/main"
NOTEBOOK_TITLE = "JMA Datasets Overview"
NOTEBOOK_SLUG = "jma-datasets-overview"
NOTEBOOK_FILE = "jma_datasets_overview.ipynb"

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "notebooks" / "jma_datasets_overview"

# Map jma_api_client module suffix → display category
_MODULE_TO_CATEGORY: dict[str, str] = {
    "japan_earthquakes": "Earthquakes & Seismic",
    "japan_volcanoes": "Volcanoes & Eruptions",
    "japan_sea": "Tsunamis & Marine",
    "japan_marine": "Tsunamis & Marine",
    "japan_weather": "Weather & Hazards",
    "japan_hazards": "Weather & Hazards",
    "japan_typhoon": "Typhoons & Forecasts",
    "japan_forecasts": "Typhoons & Forecasts",
    "japan_phenology": "Phenology & Observations",
    "japan_notices": "Informational Notices",
}

# Display order for categories
_CATEGORY_ORDER = [
    "Earthquakes & Seismic",
    "Volcanoes & Eruptions",
    "Tsunamis & Marine",
    "Weather & Hazards",
    "Typhoons & Forecasts",
    "Phenology & Observations",
    "Informational Notices",
]


def _cell_id(index: int) -> str:
    return f"cell-{index:04d}"


def markdown_cell(source: str, index: int) -> dict:
    return {
        "cell_type": "markdown",
        "id": _cell_id(index),
        "metadata": {},
        "source": source,
    }


def code_cell(source: str, index: int) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "id": _cell_id(index),
        "metadata": {},
        "outputs": [],
        "source": source,
    }


def _dataset_category(cls) -> str:
    """Return display category for a dataset class based on its module name."""
    module = cls.__module__  # e.g. 'jma_api_client.japan_earthquakes'
    suffix = module.split(".")[-1]
    return _MODULE_TO_CATEGORY.get(suffix, "Other")


def build_dataset_table(username: str) -> str:
    """Build a markdown table of all datasets grouped by category."""
    # Group datasets by category
    by_category: dict[str, list] = {cat: [] for cat in _CATEGORY_ORDER}
    by_category["Other"] = []

    for name, cls in sorted(DATASET_REGISTRY.items()):
        cat = _dataset_category(cls)
        if cat not in by_category:
            by_category[cat] = []
        by_category[cat].append(cls)

    lines = []
    for cat in _CATEGORY_ORDER:
        datasets = by_category.get(cat, [])
        if not datasets:
            continue
        lines.append(f"### {cat}\n")
        lines.append("| Dataset | Description |\n")
        lines.append("|---------|-------------|\n")
        for cls in sorted(datasets, key=lambda c: c.TITLE):
            slug = f"{username}/{cls.NAME}"
            kaggle_url = f"https://www.kaggle.com/datasets/{slug}"
            subtitle = cls.SUBTITLE or cls.TITLE
            lines.append(f"| [{cls.TITLE}]({kaggle_url}) | {subtitle} |\n")
        lines.append("\n")

    return "".join(lines)


def build_notebook(username: str) -> dict:
    """Build the nbformat v4 notebook."""
    dataset_count = len(DATASET_REGISTRY)
    table_md = build_dataset_table(username)

    cells = [
        code_cell(
            "import requests\n"
            "from IPython.display import Markdown, display\n"
            "\n"
            f'response = requests.get("{GITHUB_RAW_BASE}/README.md")\n'
            "response.raise_for_status()\n"
            "display(Markdown(response.text))\n",
            0,
        ),
        markdown_cell(
            f"---\n\n"
            f"## All {dataset_count} JMA Datasets\n\n"
            f"The table below lists every dataset produced by this pipeline, "
            f"organised by category. Click any title to open it on Kaggle.\n\n"
            + table_md
            + f"---\n\n"
            f"**Source code:** [{GITHUB_REPO_URL}]({GITHUB_REPO_URL})  \n"
            f"**Data source:** 気象庁防災情報XMLフォーマット データ を加工して作成  \n"
            f"**License:** [Public Data License v1.0](https://www.jma.go.jp/jma/kishou/info/coment.html)",
            1,
        ),
    ]

    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "name": "python",
                "version": "3.11.0",
            },
        },
        "cells": cells,
    }


def write_kernel_metadata(username: str) -> None:
    """Write kernel-metadata.json for the Kaggle Kernels API."""
    metadata = {
        "id": f"{username}/{NOTEBOOK_SLUG}",
        "title": NOTEBOOK_TITLE,
        "code_file": NOTEBOOK_FILE,
        "language": "python",
        "kernel_type": "notebook",
        "enable_gpu": False,
        "enable_tpu": False,
        "is_private": False,
        "enable_internet": True,
        "dataset_sources": [],
        "competition_sources": [],
        "kernel_sources": [],
    }
    with open(OUTPUT_DIR / "kernel-metadata.json", "w", encoding="utf-8") as fh:
        json.dump(metadata, fh, indent=2)


def push_notebook() -> bool:
    """Push the notebook to Kaggle via CLI. Returns True on success."""
    result = subprocess.run(
        [sys.executable, "-m", "kaggle.cli", "kernels", "push", "-p", str(OUTPUT_DIR)],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    output = (result.stdout + result.stderr).strip()
    if result.returncode == 0:
        print(f"Pushed notebook: {output}")
        return True
    else:
        print(f"Push failed (rc={result.returncode}): {output}", file=sys.stderr)
        return False


def main() -> None:
    username = os.environ.get("KAGGLE_USERNAME")
    if not username:
        print("Error: KAGGLE_USERNAME environment variable is required.", file=sys.stderr)
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    nb = build_notebook(username)
    nb_path = OUTPUT_DIR / NOTEBOOK_FILE
    with open(nb_path, "w", encoding="utf-8") as fh:
        json.dump(nb, fh, indent=1)
    print(f"Saved notebook : {nb_path}")

    write_kernel_metadata(username)
    print(f"Saved metadata : {OUTPUT_DIR / 'kernel-metadata.json'}")

    if not push_notebook():
        sys.exit(1)


if __name__ == "__main__":
    main()
