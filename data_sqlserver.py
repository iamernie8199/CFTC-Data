# %%
import datetime
import os
import re
from zipfile import ZipFile

import pandas as pd

import requests
from sqlalchemy import create_engine
from tqdm import trange

from sql_sqlserver import *

# %%
from datalayer_sqlserver import *

# %%
def Creat(con):
    cu = con.cursor()
    create_cmds = [
        create_cftc_cit_supplement_table_cmd,
        create_cftc_futures_table_cmd,
        create_cftc_combined_table_cmd,
        create_cftc_tff_futures_table_cmd,
        create_cftc_tff_combined_table_cmd,
        create_cftc_disagg_futures_table_cmd,
        create_cftc_disagg_combined_table_cmd
    ]
    for cmd in create_cmds:
        cu.execute(cmd)
        con.commit()


def Oldest(db):
    if db == 'api_cftc_futures':
        s = datetime.date(1986, 1, 1)
    elif db == 'api_cftc_combined':
        s = datetime.date(1995, 1, 1)
    else:
        s = datetime.date(2006, 1, 1)
    return s


def DataDownload(y, db):
    if db == 'api_cftc_cit_supplement':
        url = f"https://www.cftc.gov/files/dea/history/dea_cit_xls_{str(y)}.zip"
    elif db == 'api_cftc_futures':
        if y <= 2003:
            url = f"https://www.cftc.gov/files/dea/history/deafut_xls_{str(y)}.zip"
        else:
            url = f"https://www.cftc.gov/files/dea/history/dea_fut_xls_{str(y)}.zip"
    elif db == 'api_cftc_combined':
        if y <= 2003:
            url = f'https://www.cftc.gov/files/dea/history/deacom_xls_{str(y)}.zip'
        else:
            url = f'https://www.cftc.gov/files/dea/history/dea_com_xls_{str(y)}.zip'
    elif db == 'api_cftc_tff_futures':
        if y <= 2016:
            url = "https://www.cftc.gov/files/dea/history/fin_fut_xls_2006_2016.zip"
        else:
            url = f"https://www.cftc.gov/files/dea/history/fut_fin_xls_{str(y)}.zip"
    elif db == 'api_cftc_tff_combined':
        if y <= 2016:
            url = 'https://www.cftc.gov/files/dea/history/fin_com_xls_2006_2016.zip'
        else:
            url = f'https://www.cftc.gov/files/dea/history/com_fin_xls_{str(y)}.zip'
    elif db == 'api_cftc_disagg_futures':
        if y <= 2015:
            url = 'https://www.cftc.gov/files/dea/history/fut_disagg_xls_hist_2006_2016.zip'
        else:
            url = f'https://www.cftc.gov/files/dea/history/fut_disagg_xls_{str(y)}.zip'
    elif db == 'api_cftc_disagg_combined':
        if y <= 2015:
            url = 'https://www.cftc.gov/files/dea/history/com_disagg_xls_hist_2006_2016.zip'
        else:
            url = f'https://www.cftc.gov/files/dea/history/com_disagg_xls_{str(y)}.zip'
    filename = Download(url)
    return filename, os.path.basename(url)


def Download(u):
    resp = requests.get(u)
    file_path = os.path.join(os.path.basename(u))
    with open(file_path, 'wb') as file:
        file.write(resp.content)
    with ZipFile(file_path) as zip_ref:
        filename = zip_ref.namelist()[0]
        zip_ref.extractall()
        if len(zip_ref.namelist()) > 1:
            os.remove(zip_ref.namelist()[1])
    return filename


def Process(p, s):
    df = pd.read_excel(p)
    df = df.drop(columns=['As_of_Date_In_Form_YYMMDD'])
    header = df.columns.tolist()
    dict = {}
    for h in header:
        if re.search(r'Report_Date_as', h):
            dict[h] = 'date'
        elif h[-1] == ' ':
            dict[h] = h[:-1].lower()
        elif re.search(r'Spead', h):
            dict[h] = (re.sub(r'Spead', 'Spread', h)).lower()
        elif re.search(r'__', h):
            dict[h] = (re.sub(r'__', '_', h)).lower()
        else:
            dict[h] = h.lower()
    df = df.rename(columns=dict)
    df = df[df.date > pd.Timestamp(s)]
    df['date'] = df['date'].apply(lambda x: x.strftime("%Y-%m-%d"))
    return df


def SQL(df, db):
    engine = gen_default_sa_engine()
    df.to_sql(db, engine, index=False, if_exists='append')


def main(tbl, c):
    qq = ['api_cftc_tff_futures', 'api_cftc_tff_combined']
    gg = ['api_cftc_disagg_futures', 'api_cftc_disagg_combined']
    c.execute(f"SELECT max(date) from {tbl}")
    start = c.fetchone()[0]
    if start is None:
        start = Oldest(tbl)
    end = datetime.datetime.today()
    print(f'==={tbl}===')
    for i in trange(start.year, end.year + 1):
        if tbl in qq and start.year < i <= 2016:
            continue
        if tbl in gg and start.year < i <= 2015:
            continue
        path, zpath = DataDownload(i, tbl)
        if (not (path)):
            print('Error')
            continue
        cit_df = Process(path, start)
        SQL(cit_df, tbl)
        os.remove(zpath)
        os.remove(path)

# %%
conn = conn_tds()
cur = conn.cursor()

# %%
# 建表用
Creat(conn)

# %%

tblnames = [
    'api_cftc_cit_supplement',
    'api_cftc_futures', 'api_cftc_combined',
    'api_cftc_tff_futures', 'api_cftc_tff_combined',
    'api_cftc_disagg_futures', 'api_cftc_disagg_combined'
]
for tblname in tblnames:
    main(tblname, cur)

# %%



