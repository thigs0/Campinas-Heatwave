import polars as pl
import pandas as pd
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
import urllib3
import requests

# Ignora os avisos SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def iac_table(url):
    response = requests.get(url, verify=False)
    soup = BeautifulSoup(response.text, "html.parser")
    tables = pd.read_html(str(soup))
    return tables[0]  # ou ajuste conforme a tabela desejada

out_data = "/media/thigs/data/out/IAC_data"
def get_iac_table(url:str, lat:float, lon:float)->None:
    tabelas = iac_table(url)
    tabelas.columns = ["time", "tmin", "tmax", "relative humidity min", "relative humidity max", "pr"]

    #change data type
    tabelas[["tmin", "tmax","relative humidity max", "relative humidity min", "pr"]] = tabelas[["tmin", "tmax","relative humidity max", "relative humidity min", "pr"]].astype(float)
    tabelas.loc[:, 'time'] = pd.to_datetime(tabelas.loc[:, 'time'], format='%d-%m-%y %H:%M').dt.date
    tabelas.loc[:, 'tmin'] /= 100
    tabelas.loc[:, 'tmax'] /= 100
    tabelas.loc[:, 'relative humidity max'] /= 100
    tabelas.loc[:, 'relative humidity min'] /= 100
    tabelas.loc[:, 'pr'] /= 100
    tabelas.loc[:, 'lat'] = np.ones(len(tabelas.loc[:, 'time']))*lat
    tabelas.loc[:, 'lon'] = np.ones(len(tabelas.loc[:, 'time']))*lon
    with open(f'{out_data}/log.csv', 'a') as w:
        w.write(f"\n downloaded table of IAC site | {datetime.today()}")
        w.close()
    return tabelas

def update_dataset_campinas() ->None:
    """This function get new data of temperature, precipitation and relative humidity
    """
    #Campinas
    df = pl.read_parquet(f"{out_data}/campinas.parquet")
    out = df.join(pl.from_pandas(get_iac_table(url='https://clima.iac.sp.gov.br/index.php/main/detalhar/3',
                                               lat=-47.072878, lon=-22.867442)), on="time", how="anti")
    df = df.vstack(out)
    with open(f'{out_data}/log.csv', 'a') as w:
        w.write(f"\n insert iac data at dataset | {datetime.today()}")
        w.close()
    df.write_parquet(f"{out_data}/campinas.parquet")
    
def update_dataset_piracicaba() ->None:
    """This function get new data of temperature, precipitation and relative humidity
    """
    #Campinas
    df = pl.read_parquet(f"{out_data}/piracicaba.parquet")
    #df = pl.from_pandas(get_iac_table(url='https://clima.iac.sp.gov.br/index.php/main/detalhar/59', lat=-47.646800, lon= -22.683700))
    out = df.join(pl.from_pandas(get_iac_table(url='https://clima.iac.sp.gov.br/index.php/main/detalhar/59',
                                               lat=-47.646800, lon= -22.683700)), on="time", how="anti")
    df = df.vstack(out)
    with open(f'{out_data}/log.csv', 'a') as w:
        w.write(f"\n insert iac data at dataset | {datetime.today()}")
        w.close()
    df.write_parquet(f"{out_data}/piracicaba.parquet")

update_dataset_campinas()
update_dataset_piracicaba()


