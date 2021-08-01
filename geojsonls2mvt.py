import os
import subprocess
import glob

geojsonls = glob.glob(os.path.join(
    'geojsonl', '**', '*.geojsonl'), recursive=True)

cmd = ['tippecanoe', '-e', 'meshes', '-P',
       '-Z8', '-z8', '-pf', '-pk', '--force']

for geojsonl in geojsonls:
    layer_name = os.path.basename(geojsonl).split('.')[0].replace('-', '')
    cmd += ['-L', f'{layer_name}:{geojsonl}']

print(cmd)

subprocess.run(cmd)
