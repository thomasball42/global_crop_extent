import rasterio
from rasterio.windows import Window
from rasterio.transform import from_bounds
import numpy as np
import os
import tqdm
import multiprocessing

NUM_WORKERS = 120
year = 2019
target_dir = "data/potapov2022"
out_path = f"data/processed/Global_cropland_{year}.tif"
dst_crs = 'EPSG:4326'
tile_size = 18000

def get_total_bounds_and_res(files):
    with rasterio.open(files[0]) as src:
        bounds = src.bounds
        res = src.res
    for file in files[1:]:
        with rasterio.open(file) as src:
            bounds = (
                min(bounds[0], src.bounds.left),
                min(bounds[1], src.bounds.bottom),
                max(bounds[2], src.bounds.right),
                max(bounds[3], src.bounds.top)
            )

    return bounds, res

def process_tile(args):
    i, j, tile_width, tile_height, out_meta, f = args
    window = Window(i * tile_width, j * tile_height, tile_width, tile_height)
    mosaic_tile = np.zeros((out_meta['count'], tile_height, tile_width), dtype=out_meta['dtype'])
    
    for file in f:
        with rasterio.open(file) as src:
            src_width = src.width
            src_height = src.height
            col_off = max(window.col_off, 0)
            row_off = max(window.row_off, 0)
            width = min(window.width, src_width - col_off)
            height = min(window.height, src_height - row_off)

            if width > 0 and height > 0:
                src_window = Window(col_off, row_off, width, height)
                out_shape = (out_meta['count'], height, width)
                tile_window = src.read(window=src_window, out_shape=out_shape)
                mosaic_tile[:, :height, :width] += tile_window
                
    return mosaic_tile, window

f = []
for path, subdirs, files in os.walk(target_dir):
    for name in files:
        f.append(os.path.join(path, name))
f = [file for file in f if ".tif" in file and str(year) in file and "3km" not in file and ".aux" not in file]

total_bounds, res = get_total_bounds_and_res(f)
total_width = int((total_bounds[2] - total_bounds[0]) / res[0])
total_height = int((total_bounds[3] - total_bounds[1]) / res[1])
tile_width = tile_size
tile_height = tile_size

num_tiles_x = int(np.ceil(total_width / tile_width))
num_tiles_y = int(np.ceil(total_height / tile_height))

with rasterio.open(f[0]) as src:
    out_meta = src.meta.copy()
    out_meta.update({
        "driver": "GTiff",
        "height": num_tiles_y * tile_height,
        "width": num_tiles_x * tile_width,
        "transform": from_bounds(*total_bounds, num_tiles_x * tile_width, num_tiles_y * tile_height),
        "crs": dst_crs
    })

args = [(i, j, tile_width, tile_height, out_meta, f) for i in range(num_tiles_x) for j in range(num_tiles_y)]

with rasterio.open(out_path, 'w', **out_meta) as dest:
    with multiprocessing.Pool(processes=NUM_WORKERS) as pool:
        for result in tqdm.tqdm(pool.imap(process_tile, args), total=len(args)):
            mosaic_tile, window = result
            dest.write(mosaic_tile, window=window)
