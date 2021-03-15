import requests
import json
import argparse
import csv
import random
from functools import partial
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import pandas as pd

def read_csv_line(file_path, header=True):
    with open(file_path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for line in csv_reader:
            if header:
                header = False
            else:
                yield line

def convert_address_to_coordinates(address=None):
    try:
        api_url = "http://localhost:8088/search.php?q={address}".format(address=address)
        req = requests.get(url=api_url)
        data = data = pd.read_json(req.text)
        lat = data.lat.loc[0]
        lon = data.lon.loc[0]
    except Exception as e:
        lat, lon = None, None

    return {
        "direccion":address.split(",")[0],
        "comuna":address.split(",")[1].replace(" ",""),
        "lat":lat,
        "lon":lon
    }

def write_csv_file(output_file, dict_data, column_names):
    try:
        with open(output_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=column_names)
            writer.writeheader()
            for data in dict_data:
                writer.writerow(data)
    except IOError:
        print("I/O error") 

def main(args):
    
    p = Pool(cpu_count())
    convert_address = partial(convert_address_to_coordinates)
    direcciones = ["{0}, {1}".format(data[0],data[1]) for data in read_csv_line(args.input_file)]
    
    dict_data = list(tqdm(p.imap(convert_address, direcciones), total=len(direcciones)))
    p.close()
    p.join()

    column_names = ['direccion','comuna','lat','lon']
    write_csv_file(output_file=args.output_file, dict_data=dict_data, column_names=column_names)

if __name__ == "__main__":
    #El archivo de input debe ser un csv con las columnas [direccion, comuna]
    #python get_lat_long_from_direction.py --input_file ÑUÑOA_padron.csv --output_file nunoa_geocode.csv
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', type=str)
    parser.add_argument('--output_file', type=str)
    args = parser.parse_args()
    main(args)