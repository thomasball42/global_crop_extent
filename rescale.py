import rasterio
from rasterio.enums import Resampling
import rasterio.windows
# from rasterio.windows import Window
# from rasterio.windows import from_bounds as window_from_bounds
from rasterio.transform import from_bounds
import numpy as np
import os
import tqdm
import multiprocessing
import sys

NUM_WORKERS = 10
input_raster = "data/jung/iucn_habitatclassification_composite_lvl2_ver004.tif"
target_raster = "/maps/tsb42/bd_opp_cost/v4/agri_intersect/inputs/deltap_all_species.tif" ## TARGET A RASTER WITH A RES YOU WANT
out_path = "data/processed/jung_lvl2_1arc.tif"
dst_crs = 'EPSG:4326'
tile_size = 1000
scale_div = 5

resampling_method = Resampling.nearest ### CORRECT SAMPLING ESSENTIAL

def get_target_size_trans_bounds_res(target_raster, scale_div):
    """This is a bit janky, use e.g. scale_div=5 and a target raster of 5arcmin gets you to 1arcmin"""
    with rasterio.open(target_raster) as src:
        target_width, target_height = src.width * scale_div, src.height * scale_div
        target_bounds = src.bounds
        target_transform = src.transform
        #w, s, e, n, width, height 
        target_transform = from_bounds(*target_bounds, target_width, target_height)
        target_res = tuple(r / scale_div for r in src.res)
        target_crs = src.crs
    return target_width, target_height, target_transform, target_crs, target_bounds, target_res

def process_tile(args):
    """This might break in cases where the resolutions aren't divisible"""
    i, j, tile_width, tile_height, out_meta = args

    output_window = rasterio.windows.Window(i * tile_width, j * tile_height, tile_width, tile_height)
    output_window_bounds = rasterio.windows.bounds(output_window, transform=out_meta['transform'])

    with rasterio.open(input_raster) as src:
        in_window = rasterio.windows.from_bounds(*output_window_bounds, transform=src.transform)
        data = src.read(window=in_window, out_shape=(
            src.count, tile_height, tile_width), resampling=resampling_method)
    return data, output_window

target_width, target_height, target_transform, target_crs, total_bounds, res = get_target_size_trans_bounds_res(target_raster, scale_div)

total_width = int((- total_bounds.left + total_bounds.right) / res[0])
total_height = int((- total_bounds.bottom + total_bounds.top) / res[1])
tile_width = tile_size
tile_height = tile_size

num_tiles_x = int(np.ceil(total_width / tile_width))
num_tiles_y = int(np.ceil(total_height / tile_height))

with rasterio.open(target_raster) as target_src:
    out_meta = target_src.meta.copy()
    out_meta.update({
        "driver": "GTiff",
        "height": num_tiles_y * tile_height,
        "width": num_tiles_x * tile_width,
        "transform": target_transform,
        "crs": target_crs
    })

args = [(i, j, tile_width, tile_height, out_meta) for i in range(num_tiles_x) for j in range(num_tiles_y)]

with rasterio.open(out_path, 'w', **out_meta) as dest:
    with multiprocessing.Pool(processes=NUM_WORKERS) as pool:
        for result in tqdm.tqdm(pool.imap(process_tile, args), total=len(args)):
            mosaic_tile, window = result
            dest.write(mosaic_tile, window=window)