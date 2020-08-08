import pandas as pd
from sqlalchemy import create_engine
import requests
from zipfile import ZipFile
import os
import psycopg2
from config import pg_config
from sql import *
import datetime


def CITDownload(y):
    if y < 2006 or y > 2020:
        return 0
    print("下載" + str(y) + "年報告中...")
    url = "https://www.cftc.gov/files/dea/history/dea_cit_xls_" + str(
        y) + ".zip"
    resp = requests.get(url)
    file_path = os.path.join(os.path.basename(url))
    with open(file_path, 'wb') as file:
        file.write(resp.content)

    with ZipFile(file_path) as zip_ref:
        filename = zip_ref.namelist()[0]
        zip_ref.extractall()
    return filename


if __name__ == '__main__':
    conn = psycopg2.connect(user=pg_config['user'],
                            password=pg_config['password'],
                            host=pg_config['host'],
                            port=pg_config['port'],
                            database=pg_config['dbname'])
    cur = conn.cursor()

    # 建表用
    cur.execute(create_cftc_cit_supplement_table_cmd)
    conn.commit()

    # 取得最新日期
    cur.execute("SELECT max(date) from cftc_cit_supplement")
    start = cur.fetchone()[0]
    if start is None:
        start = datetime.date(2006, 1, 1)
    end = datetime.datetime.today()

    for i in range(start.year, end.year + 1):
        path = CITDownload(i)
        if (not (path)):
            print('Error')
            continue
        df = pd.read_excel(path)
        df = df.drop(columns=['As_of_Date_In_Form_YYMMDD'])
        header = df.columns.tolist()
        header = header[1:2] + header[0:1] + header[2:]
        dict = {}
        for h in header:
            if h == 'Report_Date_as_MM_DD_YYYY' or h == 'Report_Date_as_YYYY_MM_DD':
                dict[h] = 'date'
            elif h[-1] == ' ':
                dict[h] = h[:-1].lower()
            elif h == 'Change_NonComm_Spead_All_NoCIT':
                dict[h] = 'Change_NonComm_Spread_All_NoCIT'.lower()
            else:
                dict[h] = h.lower()
        df = df[header]
        df = df.rename(columns=dict)
        df = df[df.date > pd.Timestamp(start)]
        df['date'] = df['date'].apply(lambda x: x.strftime("%Y-%m-%d"))

        engine = create_engine('postgres://' + pg_config['user'] + ':' +
                               pg_config['password'] + '@' +
                               pg_config['host'] + ':' +
                               str(pg_config['port']) + '/' +
                               pg_config['dbname'])
        df.to_sql('cftc_cit_supplement',
                  engine,
                  index=False,
                  if_exists='append')
        print(str(i) + '年 Done')
        os.remove("dea_cit_xls_" + str(i) + ".zip")
        os.remove(path)
