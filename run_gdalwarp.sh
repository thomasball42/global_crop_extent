#!/bin/bash
# This just runs gdalwarp in a way that's easier to use than typing 100000 things into the terminal

# input_file="data/processed/Global_cropland_2003.tif"
# output_file="data/1arc/Global_cropland_2003_1arc.tif"

output_dir="data/potapov2022_1arc"


if [ $# -eq 0 ]; then
    echo "No input file provided. Usage: $0 <input_file>"
    exit 1
fi

input_file="$1"

input_basename=$(basename "$input_file" .tif)
output_file="${output_dir}/${input_basename}_1arc.tif"

t_srs="EPSG:4326"
# ts="21600 10800"
# te="-180.000000000000000 -90.000000000000000 180.000000000000000 90.000000000000000"
tr="0.0166666666666666 0.0166666666666666"

num_threads="30"
cache_max="500000"
working_memory="5000000"
resampling_method="sum"
overwrite="-o"

gdalwarp "$input_file" "$output_file" \
    -t_srs $t_srs \
    -tr $tr \
    -wo NUM_THREADS=$num_threads \
    -co NUM_THREADS=$num_threads \
    -multi \
    --config GDAL_CACHEMAX $cache_max \
    -wm $working_memory \
    -r $resampling_method \
    -overwrite
exit 0