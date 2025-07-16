import setuptools

# Read the contents of the README.md file for the long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    # This is the name of your project. It will be used as the package name on PyPI.
    name="geohashtree",

    # The version of your package.
    version="0.1.0",

    # The author's name.
    author="Your Name",

    # The author's email address.
    author_email="your.email@example.com",

    # A short, one-sentence summary of your project.
    description="A Python library for spatial indexing and querying with Geohash trees.",

    # A long description for your project, which will be displayed on PyPI.
    # We are using the content of the README.md file.
    long_description=long_description,

    # Specifies that the long description is in Markdown format.
    long_description_content_type="text/markdown",

    # The URL for your project's homepage.
    url="https://github.com/your-username/geohashtree",

    # Find all packages in the project automatically.
    # This will discover the 'geohashtree' directory and include it.
    packages=setuptools.find_packages(),

    # A list of classifiers that categorize your project.
    # See https://pypi.org/classifiers/ for a full list.
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: GIS",
    ],

    # Specifies the minimum version of Python required.
    python_requires='>=3.6',

    # A list of dependencies that your project needs to run.
    # These will be installed by pip when your project is installed.
    install_requires=[
        "pygeohash @ git+https://github.com/wdm0006/pygeohash.git", # Geohash library for Python
        "argparse", # For parsing command-line arguments
        "geopandas", # For handling geospatial data
        "parquet-tools", # For reading and writing Parquet files
        "psycopg2-binary", # PostgreSQL adapter for Python
        "jupyter", # For Jupyter notebook support
        "leafmap", # For interactive maps
        "tqdm", # For progress bars
        "h3", # H3 geospatial indexing library
    ],

    # This section defines the command-line script.
    # 'console_scripts' creates an executable script that can be run from the command line.
    entry_points={
        'console_scripts': [
            # This line creates the 'geohashtree' command.
            # When the user runs 'geohashtree', it will call the 'main' function
            # in the 'geohashtree.cli' module.
            'geohashtree = geohashtree.cli:main',
        ],
    },
)
