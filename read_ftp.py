import urllib.request
import os
from  datetime import datetime
import json
#
output_folder = 'ftp_files'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)
print("ABC3")
#
path_json = "creds.json"
# Opening JSON file
with open(path_json) as my_file:
    data = json.load(my_file)
    print(my_file.read())
print(data)
def read_ftp(file_name: str):
    url = f"{data["ftp_address"]}/"
    path_out = f"{output_folder}/{file_name}"
    urllib.request.urlretrieve(url, path_out)

def download_csv(file_name: str):
    url = f"{data["ftp_address"]}/{file_name}"
    path_out = f"{output_folder}/{file_name}"
    urllib.request.urlretrieve(url, path_out)

download_csv('relatorio_sagres_coordernadores_2024_02_29.csv')
read_ftp('files.txt')
