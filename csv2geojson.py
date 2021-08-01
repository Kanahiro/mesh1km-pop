import os
import glob
import zipfile
import json
from functools import lru_cache

import pandas as pd
import jismesh.utils as ju
import tqdm

to_meshpoint = lru_cache(10)(ju.to_meshpoint)

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

# csv2geojsonl
output_dir = 'geojsonl'
os.makedirs(output_dir, exist_ok=True)

csv_files = glob.glob(os.path.join('csv', '**', '*.csv'), recursive=True)
for csv_file in tqdm.tqdm(csv_files):
    csv_df = pd.read_csv(csv_file, dtype=str).astype({'population': int})
    csv_df['dt_key'] = 'd' + csv_df['dayflag'] + 't' + csv_df['timezone']

    meshcode_df = csv_df[['mesh1kmid', 'citycode']
                         ].drop_duplicates(subset='mesh1kmid').copy()
    meshcode_df['geometry'] = meshcode_df['mesh1kmid'].map(
        lambda meshcode: ((
            tuple(reversed(to_meshpoint(meshcode, 0, 0))),
            tuple(reversed(to_meshpoint(meshcode, 0, 1))),
            tuple(reversed(to_meshpoint(meshcode, 1, 1))),
            tuple(reversed(to_meshpoint(meshcode, 1, 0))),
            tuple(reversed(to_meshpoint(meshcode, 0, 0))),
        ),),)

    def get_population(meshcode):
        filtered = csv_df[(csv_df['mesh1kmid'] == meshcode)
                          ][['dt_key', 'population']]

        filtered_dict = filtered.to_dict(orient='list')
        populations = dict(
            zip(filtered_dict['dt_key'], filtered_dict['population']))

        return pd.Series([
            populations.get('d0t0', 0),
            populations.get('d0t1', 0),
            populations.get('d0t2', 0),
            populations.get('d1t0', 0),
            populations.get('d1t1', 0),
            populations.get('d1t2', 0),
            populations.get('d2t0', 0),
            populations.get('d2t1', 0),
            populations.get('d2t0', 0),
        ])

    meshcode_df[['d0t0',
                 'd0t1',
                 'd0t2',
                 'd1t0',
                 'd1t1',
                 'd1t2',
                 'd2t0',
                 'd2t1',
                 'd2t2']] = meshcode_df['mesh1kmid'].apply(get_population)

    records = meshcode_df.to_dict(orient='records')

    paths = csv_file.split(os.path.sep)
    year_month = paths[1][3:]
    with open(os.path.join(output_dir, year_month + '.geojsonl'), mode='a') as f:
        for record in records:
            geojson = {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": record["geometry"]
                },
                "properties": {
                    "citycode": record["citycode"],
                    "d0t0": record["d0t0"],
                    "d0t1": record["d0t1"],
                    "d0t2": record["d0t2"],
                    "d1t0": record["d1t0"],
                    "d1t1": record["d1t1"],
                    "d1t2": record["d1t2"],
                    "d2t0": record["d2t0"],
                    "d2t1": record["d2t1"],
                    "d2t2": record["d2t2"],
                }
            }
            geojson_str = json.dumps(geojson)
            f.write(geojson_str + '\n')
