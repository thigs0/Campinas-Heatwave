import polars as pl
import pandas as pd
import numpy as np
from datetime import datetime

out_data = "/media/thigs/data/out/IAC_data"

def get_iac_table(url:str, lat:float, lon:float)->None:
    tabelas = pd.read_html(url)
    tabelas[0].columns = ["time", "tmin", "tmax", "relative humidity min", "relative humidity max", "pr"]

    #change data type
    tabelas[0][["tmin", "tmax","relative humidity max", "relative humidity min", "pr"]] = tabelas[0][["tmin", "tmax","relative humidity max", "relative humidity min", "pr"]].astype(float)
    tabelas[0].loc[:, 'time'] = pd.to_datetime(tabelas[0].loc[:, 'time'], format='%d-%m-%y %H:%M').dt.date
    tabelas[0].loc[:, 'tmin'] /= 100
    tabelas[0].loc[:, 'tmax'] /= 100
    tabelas[0].loc[:, 'relative humidity max'] /= 100
    tabelas[0].loc[:, 'relative humidity min'] /= 100
    tabelas[0].loc[:, 'pr'] /= 100
    tabelas[0].loc[:, 'lat'] = np.ones(len(tabelas[0].loc[:, 'time']))*lat
    tabelas[0].loc[:, 'lon'] = np.ones(len(tabelas[0].loc[:, 'time']))*lon
    with open(f'{out_data}/log.csv', 'a') as w:
        w.write(f"\n downloaded table of IAC site | {datetime.today()}")
        w.close()
    return tabelas[0]

def update_dataset_campinas() ->None:
    """This function get new data of temperature, precipitation and relative humidity
    """
    #Campinas
    df = pl.read_parquet(f"{out_data}/campinas.parquet")
    df = print(pl.from_pandas(get_iac_table(url='https://clima.iac.sp.gov.br/index.php/main/detalhar/3', lat=-47.072878, lon=-22.867442)))
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


