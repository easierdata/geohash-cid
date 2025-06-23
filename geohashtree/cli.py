import argparse
import sys

from geohashtree.geohashtree import LiteTreeOffset, LiteTreeCID
from geohashtree.filesystem import ipfs_add_feature,ipfs_add_index_folder,kubo_rpc_cat_offset_length
def handle_create(args):
    """
    Placeholder function to handle the 'create' command.
    This is where you would implement the logic to build the GeohashTree index.
    """
    print("Executing 'create' command...")
    print(f"  Input file/path: {args.input_path}")
    print(f"  Output folder: {args.output_folder}")
    print(f"  Method: {args.method}")
    print(f"  File format: {args.format}")
    print(f"  Level: {args.level}")
    if args.method == "prepartition":
        print("  Using prepartition method for indexing.")
        geohashtree = LiteTreeCID()
    elif args.method == "offset":
        print("  Using offset method for indexing.")
        geohashtree = LiteTreeOffset()
    else:
        print(f"  Error: Unsupported method '{args.method}'. Use 'prepartition' or 'offset'.")
        sys.exit(1)
    
    if args.format not in ["parquet", "geojson"]:
        print(f"  Error: Unsupported format '{args.format}'. Use 'parquet' or 'geojson'.")
        sys.exit(1)
    geohashtree.file_format = args.format
    geohashtree.calculate_index_from_file(
        args.input_path,
        args.output_folder,
        args.level
    )
    geohashtree.generate_tree_index(args.output_folder)
    print("\nSuccessfully created geohashtree index (simulation).")


def handle_copy(args):
    """
    Placeholder function to handle the 'copy' command.
    This is where you would copy the index folder or upload it.
    """
    print("Executing 'copy' command...")
    print(f"  Source index folder: {args.source}")
    print(f"  Destination: {args.destination}")
    if args.ipfs:
        print("  Mode: Upload to IPFS")
        added_cid = ipfs_add_index_folder(args.source)
        print(f"  Successfully uploaded index to IPFS with CID: {added_cid}")
        # --- Your IPFS upload logic goes here ---
    else:
        print("  Mode: Copy to local directory")
        # recursively copy the index folder to the destination
        raise NotImplementedError
    print("\nSuccessfully copied index (simulation).")


def handle_get(args):
    """
    Placeholder function to handle the 'get' command.
    This is where you would load the index and query for features.
    """
    print("Executing 'get' command...")
    print(f"  Loading index from: {args.index_path}")
    # --- Your index loading logic goes here ---
    # tree = GeohashTree.load(args.index_path)

    if args.geohashes:
        print(f"  Querying by geohashes: {args.geohashes}")
        # --- Logic to find data by a list of geohashes ---
    elif args.bbox:
        print(f"  Querying by bounding box: {args.bbox}")
        # --- Logic to find data within a bounding box ---
    elif args.radius:
        print(f"  Querying by radius:")
        print(f"    Center: ({args.radius[0]}, {args.radius[1]})")
        print(f"    Radius: {args.radius[2]} km")
        # --- Logic to find neighbors within a radius ---
    else:
        print("  Error: No query type specified. Use --geohashes, --bbox, or --radius.")
        sys.exit(1)
    
    print("\nSuccessfully retrieved features (simulation).")


def main():
    """
    The main entry point for the geohashtree command-line interface.
    """
    # Create the top-level parser
    parser = argparse.ArgumentParser(
        description="A CLI for creating, managing, and querying GeohashTree indices."
    )
    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    # --- Create Parser for the "create" command ---
    parser_create = subparsers.add_parser("create", help="Create a new geohashtree index from a file.")
    parser_create.add_argument("input_path", type=str, help="Path to the input file (e.g., a CSV or GeoJSON).")
    parser_create.add_argument("output_folder", type=str, help="Path to the folder where the index will be saved.")
    parser_create.add_argument("--method", type=str, default="prepartition", choices=["prepartition","offset"], help="Indexing method to use (default: 'prepartition').")
    parser_create.add_argument("--format", type=str, default="csv", choices=["parquet", "geojson"], help="Input file format (parquet or geojson).")
    parser_create.add_argument("--level", type=int, default=5, help="Geohash level/precision (default: 5).")
    parser_create.set_defaults(func=handle_create)

    # --- Create Parser for the "copy" command ---
    parser_copy = subparsers.add_parser("copy", help="Copy an index to a new location or upload to IPFS.")
    parser_copy.add_argument("source", type=str, help="Path to the source index folder.")
    parser_copy.add_argument("destination", type=str, help="Path to the destination folder or IPFS identifier.")
    parser_copy.add_argument("--ipfs", action="store_true", help="Flag to indicate the destination is IPFS.")
    parser_copy.set_defaults(func=handle_copy)

    # --- Create Parser for the "get" command ---
    parser_get = subparsers.add_parser("get", help="Get features from a geohashtree index.")
    parser_get.add_argument("index_path", type=str, help="Path to the geohashtree index folder.")
    
    # A mutually exclusive group ensures only one type of query can be run at a time.
    query_group = parser_get.add_mutually_exclusive_group(required=True)
    query_group.add_argument("--geohashes", nargs='+', metavar="G", type=str, help="One or more geohashes to query.")
    query_group.add_argument("--bbox", nargs=4, metavar=('MIN_LAT', 'MIN_LON', 'MAX_LAT', 'MAX_LON'), type=float, help="Bounding box to query (min_lat min_lon max_lat max_lon).")
    query_group.add_argument("--radius", nargs=3, metavar=('LAT', 'LON', 'KM'), type=float, help="Center point and radius in kilometers (lat lon radius_km).")
    parser_get.set_defaults(func=handle_get)

    # Parse the arguments from the command line
    args = parser.parse_args()

    # Call the appropriate handler function based on the command
    if hasattr(args, 'func'):
        args.func(args)

if __name__ == "__main__":
    # This allows the script to be run directly for testing.
    # Example: python geohashtree/cli.py create input.csv /tmp/my-index
    main()
