for file in *.geojson; do
  if [ -f "$file" ]; then
    # Add the file to IPFS and store the CID
    cid=$(ipfs add -q "$file")
    # Print the CID and file name for reference
    echo "Added $file to IPFS with CID: $cid"
  fi
done

