import argparse
import os
import sys

from geohashtree.geohashtree import LiteTreeOffset, LiteTreeCID
from geohashtree.filesystem import ipfs_add_feature,ipfs_add_index_folder,kubo_rpc_cat_offset_length, ipfs_get_index_folder
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
    if args.to_ipfs:
        if not args.source:
            print("  Error: --to_ipfs requires a source path.")
            sys.exit(1)
        if os.path.isfile(args.source):
            ext = os.path.splitext(args.source)[1].lower()
            if ext in [".geojson", ".parquet"]:
                print(f"  Detected file type: {ext}. Uploading as feature to IPFS...")
                added_cid = ipfs_add_feature(args.source)
                print(f"  Successfully uploaded feature to IPFS with CID: {added_cid}")
            else:
                print(f"  Error: Unsupported file type '{ext}'. Only .geojson or .parquet are supported for feature upload.")
                sys.exit(1)
        elif os.path.isdir(args.source):
            print("  Detected folder. Uploading index folder to IPFS...")
            added_cid = ipfs_add_index_folder(args.source)
            print(f"  Successfully uploaded index folder to IPFS with CID: {added_cid}")
        else:
            print("  Error: Source path does not exist.")
            sys.exit(1)
    elif args.from_ipfs:
        if not args.destination:
            print("  Error: --from_ipfs requires a destination path.")
            sys.exit(1)
        print("  Downloading from IPFS to local destination (not implemented).")
        # Implement IPFS download logic here
        raise NotImplementedError
    else:
        if not args.source or not args.destination:
            print("  Error: Both source and destination are required unless --to_ipfs or --from_ipfs is specified.")
            sys.exit(1)
        print("  Mode: Copy to local directory")
        # recursively copy the index folder to the destination
        raise NotImplementedError
    print("\nSuccessfully copied index .")


def handle_get(args):
    """
    Placeholder function to handle the 'get' command.
    This is where you would load the index and query for features.
    """
    print("Executing 'get' command...")
    if args.ipfs:
        mode = "online"
    else:
        mode = "offline"
        if not os.path.exists(args.index_path):
            print(f'no local index found! caching {args.index_cid} to',args.index_path)
            ipfs_get_index_folder(args.index_cid,args.index_path)
    print(f"  Mode: {mode}")
    print(f"  Loading index from: {args.index_path}")
    # --- Your index loading logic goes here ---
    # tree = GeohashTree.load(args.index_path)
    if args.method == "prepartition":
        print("  Using prepartition method for indexing.")
        geohashtree = LiteTreeCID(mode=mode)
    elif args.method == "offset":
        print("  Using offset method for indexing.")
        geohashtree = LiteTreeOffset(mode=mode)
    else:
        print(f"  Error: Unsupported method '{args.method}'. Use 'prepartition' or 'offset'.")
        sys.exit(1)
    if args.format not in ["parquet", "geojson"]:
        print(f"  Error: Unsupported format '{args.format}'. Use 'parquet' or 'geojson'.")
        sys.exit(1)
    geohashtree.file_format = args.format
    

    if args.geohashes:
        print(f"  Querying by geohashes: {args.geohashes}")
        retr = geohashtree.retrieve(args.geohashes,args.index_path)
        print('IPFS return size',retr.shape)
        print("  Features found:")
        for feature in retr.head(5).to_dict(orient='records'):
            print(f"    - {feature}")
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
    parser_create.add_argument("input_path", type=str, help="Path to the input file (e.g., a parquet or GeoJSON).")
    parser_create.add_argument("output_folder", type=str, help="Path to the folder where the index will be saved.")
    parser_create.add_argument("--method", type=str, default="prepartition", choices=["prepartition","offset"], help="Indexing method to use (default: 'prepartition').")
    parser_create.add_argument("--format", type=str, default="geojson", choices=["parquet", "geojson"], help="Input file format (parquet or geojson).")
    parser_create.add_argument("--level", type=int, default=5, help="Geohash level/precision (default: 5).")
    parser_create.set_defaults(func=handle_create)

    # --- Create Parser for the "copy" command ---
    parser_copy = subparsers.add_parser("copy", help="Copy an index to a new location or upload to IPFS.")
    parser_copy.add_argument("source", type=str, nargs="?", help="Path to the source index folder.")
    parser_copy.add_argument("destination", type=str, nargs="?", help="Path to the destination folder or IPFS identifier.")
    parser_copy.add_argument("--to_ipfs", action="store_true", help="Flag to indicate the destination is IPFS.")
    parser_copy.add_argument("--from_ipfs", action="store_true", help="Flag to indicate the source is from IPFS.")
    parser_copy.set_defaults(func=handle_copy)

    # --- Create Parser for the "get" command ---
    parser_get = subparsers.add_parser("get", help="Get features from a geohashtree index.")
    parser_get.add_argument("index_path", type=str, help="Path to the geohashtree index folder.")
    parser_get.add_argument("index_cid", type=str, help="Path to the geohashtree index folder.")
    parser_get.add_argument("--method", type=str, default="prepartition", choices=["prepartition","offset"], help="Indexing method to use (default: 'prepartition').")
    parser_get.add_argument("--format", type=str, default="geojson", choices=["parquet", "geojson"], help="Input file format (parquet or geojson).")
    parser_get.add_argument("--ipfs", action="store_true", help="Flag to indicate the index is stored on IPFS.")
    parser_get.add_argument("--kubo_rpc", type=str, default="http://localhost:5001", help="Kubo RPC endpoint for IPFS (default: http://localhost:5001).")
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
