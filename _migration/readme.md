# Geohash + CID
Enable spatial indexing in IPFS ecosystem.
[What is geohash?](https://www.educative.io/answers/what-is-geohash)

## Getting started
The project uses [Poetry](https://python-poetry.org/) for dependency management. To set up poetry, see [installation](https://python-poetry.org/docs/#installing-manually). Make sure to use python verison 3.9 or above to use geopandas 0.14.

With Poetry installed and the repository cloned, install the project dependencies:
```
poetry install
```
or for non-root install
```
poetry install --no-root
```

This command reads the pyproject.toml file and installs all required packages in a new virtual environment (including `jupyter notebook`).

Run `poetry run jupyter notebook` to start jupyter notebooks.