import os
import glob
import zipfile
import json
from functools import lru_cache
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
import jismesh.utils as ju
import tqdm

to_meshpoint = lru_cache(10)(ju.to_meshpoint)

all_csv_df_pickle = 'all_csv_df.pkl'
if os.path.exists(all_csv_df_pickle):
    all_csv_df = pd.read_pickle(all_csv_df_pickle)
else:
    # extract zip
    child_zip_files = glob.glob(os.path.join(
        'zip', '**', '*.zip'), recursive=True)
    for child_zip_file in child_zip_files:
        with zipfile.ZipFile(child_zip_file) as existing_zip:
            extract_dir = 'child_zip'
            os.makedirs('child_zip', exist_ok=True)
            existing_zip.extractall(extract_dir)

    # extract child_zip
    child_zip_files = glob.glob(os.path.join(
        'child_zip', '**', '*.zip'), recursive=True)
    for child_zip_file in child_zip_files:
        with zipfile.ZipFile(child_zip_file) as existing_zip:
            paths = child_zip_file.split(os.path.sep)
            prefcode, year, month = paths[1], paths[2], paths[3]

            extract_dir = os.path.join('csv', f'{prefcode}-{year}-{month}')
            os.makedirs(extract_dir, exist_ok=True)
            existing_zip.extractall(extract_dir)
    csv_files = glob.glob(os.path.join('csv', '**', '*.csv'), recursive=True)

    def load_df(csv_file):
        csv_df = pd.read_csv(csv_file, dtype=str).astype({'population': int})
        csv_df['dt_key'] = 'd' + csv_df['dayflag'] + 't' + csv_df['timezone']
        return csv_df

    csv_dfs = list(map(load_df, csv_files))
    all_csv_df = pd.concat(csv_dfs)
    all_csv_df["year-month-dt-key"] = all_csv_df["year"] + \
        all_csv_df["month"] + all_csv_df["dt_key"]
    all_csv_df.to_pickle(all_csv_df_pickle)

mesh_df_pickle = 'mesh_df.pkl'
if os.path.exists(mesh_df_pickle):
    mesh_df = pd.read_pickle(mesh_df_pickle)
else:
    mesh_df = all_csv_df[['mesh1kmid', 'citycode']
                         ].drop_duplicates(subset='mesh1kmid').copy()
    mesh_df['geometry'] = mesh_df['mesh1kmid'].map(
        lambda meshcode: ((
            tuple(reversed(to_meshpoint(meshcode, 0, 0))),
            tuple(reversed(to_meshpoint(meshcode, 0, 1))),
            tuple(reversed(to_meshpoint(meshcode, 1, 1))),
            tuple(reversed(to_meshpoint(meshcode, 1, 0))),
            tuple(reversed(to_meshpoint(meshcode, 0, 0))),
        ),),)
    mesh_df.to_pickle(mesh_df_pickle)

mesh_population_df_pickle = 'mesh_population_df.pkl'
if os.path.exists(mesh_population_df_pickle):
    mesh_population_df = pd.read_pickle(mesh_population_df_pickle)
else:
    mesh_population_df = all_csv_df[["mesh1kmid", "population", "year-month-dt-key"]
                                    ].pivot(values='population', index='mesh1kmid', columns='year-month-dt-key').fillna(0).astype(int)
    mesh_population_df.to_pickle(mesh_population_df_pickle)

merged = pd.merge(mesh_df, mesh_population_df.reset_index(), on="mesh1kmid")
records = merged.to_dict(orient='records')

with open('meshes.geojsonl', mode='w') as f:
    for rec in records:
        mesh_feature = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": rec["geometry"]
            },
            "properties": {
                "citycode": rec["citycode"],
                "201901d0t0": rec["201901d0t0"],
                "201901d0t1": rec["201901d0t1"],
                "201901d0t2": rec["201901d0t2"],
                "201901d1t0": rec["201901d1t0"],
                "201901d1t1": rec["201901d1t1"],
                "201901d1t2": rec["201901d1t2"],
                "201901d2t0": rec["201901d2t0"],
                "201901d2t1": rec["201901d2t1"],
                "201901d2t2": rec["201901d2t2"],
                "202001d0t0": rec["202001d0t0"],
                "202001d0t1": rec["202001d0t1"],
                "202001d0t2": rec["202001d0t2"],
                "202001d1t0": rec["202001d1t0"],
                "202001d1t1": rec["202001d1t1"],
                "202001d1t2": rec["202001d1t2"],
                "202001d2t0": rec["202001d2t0"],
                "202001d2t1": rec["202001d2t1"],
                "202001d2t2": rec["202001d2t2"],
                "201902d0t0": rec["201902d0t0"],
                "201902d0t1": rec["201902d0t1"],
                "201902d0t2": rec["201902d0t2"],
                "201902d1t0": rec["201902d1t0"],
                "201902d1t1": rec["201902d1t1"],
                "201902d1t2": rec["201902d1t2"],
                "201902d2t0": rec["201902d2t0"],
                "201902d2t1": rec["201902d2t1"],
                "201902d2t2": rec["201902d2t2"],
                "202002d0t0": rec["202002d0t0"],
                "202002d0t1": rec["202002d0t1"],
                "202002d0t2": rec["202002d0t2"],
                "202002d1t0": rec["202002d1t0"],
                "202002d1t1": rec["202002d1t1"],
                "202002d1t2": rec["202002d1t2"],
                "202002d2t0": rec["202002d2t0"],
                "202002d2t1": rec["202002d2t1"],
                "202002d2t2": rec["202002d2t2"],
                "201903d0t0": rec["201903d0t0"],
                "201903d0t1": rec["201903d0t1"],
                "201903d0t2": rec["201903d0t2"],
                "201903d1t0": rec["201903d1t0"],
                "201903d1t1": rec["201903d1t1"],
                "201903d1t2": rec["201903d1t2"],
                "201903d2t0": rec["201903d2t0"],
                "201903d2t1": rec["201903d2t1"],
                "201903d2t2": rec["201903d2t2"],
                "202003d0t0": rec["202003d0t0"],
                "202003d0t1": rec["202003d0t1"],
                "202003d0t2": rec["202003d0t2"],
                "202003d1t0": rec["202003d1t0"],
                "202003d1t1": rec["202003d1t1"],
                "202003d1t2": rec["202003d1t2"],
                "202003d2t0": rec["202003d2t0"],
                "202003d2t1": rec["202003d2t1"],
                "202003d2t2": rec["202003d2t2"],
                "201904d0t0": rec["201904d0t0"],
                "201904d0t1": rec["201904d0t1"],
                "201904d0t2": rec["201904d0t2"],
                "201904d1t0": rec["201904d1t0"],
                "201904d1t1": rec["201904d1t1"],
                "201904d1t2": rec["201904d1t2"],
                "201904d2t0": rec["201904d2t0"],
                "201904d2t1": rec["201904d2t1"],
                "201904d2t2": rec["201904d2t2"],
                "202004d0t0": rec["202004d0t0"],
                "202004d0t1": rec["202004d0t1"],
                "202004d0t2": rec["202004d0t2"],
                "202004d1t0": rec["202004d1t0"],
                "202004d1t1": rec["202004d1t1"],
                "202004d1t2": rec["202004d1t2"],
                "202004d2t0": rec["202004d2t0"],
                "202004d2t1": rec["202004d2t1"],
                "202004d2t2": rec["202004d2t2"],
                "201905d0t0": rec["201905d0t0"],
                "201905d0t1": rec["201905d0t1"],
                "201905d0t2": rec["201905d0t2"],
                "201905d1t0": rec["201905d1t0"],
                "201905d1t1": rec["201905d1t1"],
                "201905d1t2": rec["201905d1t2"],
                "201905d2t0": rec["201905d2t0"],
                "201905d2t1": rec["201905d2t1"],
                "201905d2t2": rec["201905d2t2"],
                "202005d0t0": rec["202005d0t0"],
                "202005d0t1": rec["202005d0t1"],
                "202005d0t2": rec["202005d0t2"],
                "202005d1t0": rec["202005d1t0"],
                "202005d1t1": rec["202005d1t1"],
                "202005d1t2": rec["202005d1t2"],
                "202005d2t0": rec["202005d2t0"],
                "202005d2t1": rec["202005d2t1"],
                "202005d2t2": rec["202005d2t2"],
                "201906d0t0": rec["201906d0t0"],
                "201906d0t1": rec["201906d0t1"],
                "201906d0t2": rec["201906d0t2"],
                "201906d1t0": rec["201906d1t0"],
                "201906d1t1": rec["201906d1t1"],
                "201906d1t2": rec["201906d1t2"],
                "201906d2t0": rec["201906d2t0"],
                "201906d2t1": rec["201906d2t1"],
                "201906d2t2": rec["201906d2t2"],
                "202006d0t0": rec["202006d0t0"],
                "202006d0t1": rec["202006d0t1"],
                "202006d0t2": rec["202006d0t2"],
                "202006d1t0": rec["202006d1t0"],
                "202006d1t1": rec["202006d1t1"],
                "202006d1t2": rec["202006d1t2"],
                "202006d2t0": rec["202006d2t0"],
                "202006d2t1": rec["202006d2t1"],
                "202006d2t2": rec["202006d2t2"],
                "201907d0t0": rec["201907d0t0"],
                "201907d0t1": rec["201907d0t1"],
                "201907d0t2": rec["201907d0t2"],
                "201907d1t0": rec["201907d1t0"],
                "201907d1t1": rec["201907d1t1"],
                "201907d1t2": rec["201907d1t2"],
                "201907d2t0": rec["201907d2t0"],
                "201907d2t1": rec["201907d2t1"],
                "201907d2t2": rec["201907d2t2"],
                "202007d0t0": rec["202007d0t0"],
                "202007d0t1": rec["202007d0t1"],
                "202007d0t2": rec["202007d0t2"],
                "202007d1t0": rec["202007d1t0"],
                "202007d1t1": rec["202007d1t1"],
                "202007d1t2": rec["202007d1t2"],
                "202007d2t0": rec["202007d2t0"],
                "202007d2t1": rec["202007d2t1"],
                "202007d2t2": rec["202007d2t2"],
                "201908d0t0": rec["201908d0t0"],
                "201908d0t1": rec["201908d0t1"],
                "201908d0t2": rec["201908d0t2"],
                "201908d1t0": rec["201908d1t0"],
                "201908d1t1": rec["201908d1t1"],
                "201908d1t2": rec["201908d1t2"],
                "201908d2t0": rec["201908d2t0"],
                "201908d2t1": rec["201908d2t1"],
                "201908d2t2": rec["201908d2t2"],
                "202008d0t0": rec["202008d0t0"],
                "202008d0t1": rec["202008d0t1"],
                "202008d0t2": rec["202008d0t2"],
                "202008d1t0": rec["202008d1t0"],
                "202008d1t1": rec["202008d1t1"],
                "202008d1t2": rec["202008d1t2"],
                "202008d2t0": rec["202008d2t0"],
                "202008d2t1": rec["202008d2t1"],
                "202008d2t2": rec["202008d2t2"],
                "201909d0t0": rec["201909d0t0"],
                "201909d0t1": rec["201909d0t1"],
                "201909d0t2": rec["201909d0t2"],
                "201909d1t0": rec["201909d1t0"],
                "201909d1t1": rec["201909d1t1"],
                "201909d1t2": rec["201909d1t2"],
                "201909d2t0": rec["201909d2t0"],
                "201909d2t1": rec["201909d2t1"],
                "201909d2t2": rec["201909d2t2"],
                "202009d0t0": rec["202009d0t0"],
                "202009d0t1": rec["202009d0t1"],
                "202009d0t2": rec["202009d0t2"],
                "202009d1t0": rec["202009d1t0"],
                "202009d1t1": rec["202009d1t1"],
                "202009d1t2": rec["202009d1t2"],
                "202009d2t0": rec["202009d2t0"],
                "202009d2t1": rec["202009d2t1"],
                "202009d2t2": rec["202009d2t2"],
                "201910d0t0": rec["201910d0t0"],
                "201910d0t1": rec["201910d0t1"],
                "201910d0t2": rec["201910d0t2"],
                "201910d1t0": rec["201910d1t0"],
                "201910d1t1": rec["201910d1t1"],
                "201910d1t2": rec["201910d1t2"],
                "201910d2t0": rec["201910d2t0"],
                "201910d2t1": rec["201910d2t1"],
                "201910d2t2": rec["201910d2t2"],
                "202010d0t0": rec["202010d0t0"],
                "202010d0t1": rec["202010d0t1"],
                "202010d0t2": rec["202010d0t2"],
                "202010d1t0": rec["202010d1t0"],
                "202010d1t1": rec["202010d1t1"],
                "202010d1t2": rec["202010d1t2"],
                "202010d2t0": rec["202010d2t0"],
                "202010d2t1": rec["202010d2t1"],
                "202010d2t2": rec["202010d2t2"],
                "201911d0t0": rec["201911d0t0"],
                "201911d0t1": rec["201911d0t1"],
                "201911d0t2": rec["201911d0t2"],
                "201911d1t0": rec["201911d1t0"],
                "201911d1t1": rec["201911d1t1"],
                "201911d1t2": rec["201911d1t2"],
                "201911d2t0": rec["201911d2t0"],
                "201911d2t1": rec["201911d2t1"],
                "201911d2t2": rec["201911d2t2"],
                "202011d0t0": rec["202011d0t0"],
                "202011d0t1": rec["202011d0t1"],
                "202011d0t2": rec["202011d0t2"],
                "202011d1t0": rec["202011d1t0"],
                "202011d1t1": rec["202011d1t1"],
                "202011d1t2": rec["202011d1t2"],
                "202011d2t0": rec["202011d2t0"],
                "202011d2t1": rec["202011d2t1"],
                "202011d2t2": rec["202011d2t2"],
                "201912d0t0": rec["201912d0t0"],
                "201912d0t1": rec["201912d0t1"],
                "201912d0t2": rec["201912d0t2"],
                "201912d1t0": rec["201912d1t0"],
                "201912d1t1": rec["201912d1t1"],
                "201912d1t2": rec["201912d1t2"],
                "201912d2t0": rec["201912d2t0"],
                "201912d2t1": rec["201912d2t1"],
                "201912d2t2": rec["201912d2t2"],
                "202012d0t0": rec["202012d0t0"],
                "202012d0t1": rec["202012d0t1"],
                "202012d0t2": rec["202012d0t2"],
                "202012d1t0": rec["202012d1t0"],
                "202012d1t1": rec["202012d1t1"],
                "202012d1t2": rec["202012d1t2"],
                "202012d2t0": rec["202012d2t0"],
                "202012d2t1": rec["202012d2t1"],
                "202012d2t2": rec["202012d2t2"],
            }
        }
        f.write(json.dumps(mesh_feature) + '\n')
