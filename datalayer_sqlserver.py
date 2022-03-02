# %%
#config_loc = "config\\config.yml"

# %%
#%run func_config.ipynb
#%run func_df.ipynb

# %%
#%run load_func_local.ipynb

# %%
from config_sqlserver import global_cfg
import petl as etl
import sys
import os

# %%
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy import event
from sqlalchemy import select
from sqlalchemy.inspection import inspect
import pytds
import sqlalchemy_pytds
import re

# %%
import traceback

# %%
sys.path.append(os.path.realpath('.'))

# %%
def exn_handle(e):
    error_class = e.__class__.__name__  # 取得錯誤類型
    detail = e.args[0]  # 取得詳細內容
    cl, exc, tb = sys.exc_info()  # 取得Call Stack
    lastCallStack = traceback.extract_tb(tb)[-1]  # 取得Call Stack的最後一筆資料
    fileName = lastCallStack[0]  # 取得發生的檔案名稱
    lineNum = lastCallStack[1]  # 取得發生的行號
    funcName = lastCallStack[2]  # 取得發生的函數名稱
    errMsg = "File \"{}\", line {}, in {}: [{}] {}".format(fileName, lineNum, funcName, error_class, detail)
    return errMsg

# %%
def conn_tds(to=30, cfg=global_cfg):
    return pytds.connect(dsn=cfg.pyExec_host,
                       database=cfg.pyExec_db,
                       user=cfg.pyExec_uid,
                       password=cfg.pyExec_upwd,
                       port=cfg.pyExec_port,
                       timeout=to)

# %%
def exe(s, p, to, cfg=global_cfg, exn_handler=lambda exn: print(exn_handle(exn))):
    if to is None:
        to = 30
    with conn_tds(to, cfg) as conn:
        conn.set_autocommit(False)
        try:
            engine = conn.cursor()
            ex_rst = engine.execute(s, p)
            rc = ex_rst._session.rows_affected
            conn.commit()
            print("committed: ", s, rc)
            return ex_rst, rc
        except Exception as exn:
            conn.rollback()
            print("rollbacked: ", s)
            exn_handler(exn)

# %%
def qry_with_conn(s, conn, p={}, to=None, max_next_set=5):
    if to is None:
        to = 30
    # conn.mars_enabled == True
    engine = conn.cursor()
    qry_rst = engine.execute(s, p)
    fetched_rst = None
    try:
        print("first fetchall")
        fetched_rst = qry_rst.fetchall()
        print("first fetchall done")
    except Exception as ex:
        while fetched_rst is None and max_next_set > 0:
            try:
                qry_rst.nextset()
                max_next_set -= 1
                print("nextset")
                fetched_rst = qry_rst.fetchall()
            except:
                pass

    # from pprint import pprint
    # pprint(vars(qryrst))
    # print("qryrst.description", qryrst.description)
    return fetched_rst, qry_rst.description

# %%
def qry(s, p={}, to=None, cfg=global_cfg, exn_handler=lambda exn: print(exn_handle(exn))):
    if to is None:
        to = 30
    with conn_tds(to, cfg) as conn:
        conn.set_autocommit(False)
        try:
            row_list, cols_def = qry_with_conn(s, conn, p, to)
            conn.commit()
            print("committed", s)
            return row_list, cols_def
        except Exception as exn:
            conn.rollback()
            exn_handler(exn)
            return None, None

# %%
def get_sql_server_schemas(sa_engine, insp=None):
    if insp is None:
        insp = inspect(sa_engine)
    return filter(lambda sch: iif(re.match("^((db_.*)|(guest)|(INFORMATION_SCHEMA)|(sys))$", sch), False, True), insp.get_schema_names())

def get_sql_server_tbls(sa_engine):
    insp = inspect(sa_engine)
    schemas = get_sql_server_schemas(sa_engine, insp)
    return lcollect(lambda sch: map(lambda tbl: "[" + sch + "]" + "." + "[" + tbl + "]", insp.get_table_names(sch)), schemas)

# %%
def gen_default_sa_engine():
    sa_engine = create_engine('mssql+pytds://'+ 
        global_cfg.pyExec_uid + ':' + global_cfg.pyExec_upwd + '@' + global_cfg.pyExec_host + ":" + global_cfg.pyExec_port + '/' +  global_cfg.pyExec_db)
    return sa_engine

def gen_default_sa_conn():
    sa_engine = gen_default_sa_engine()
    sa_conn = sa_engine.connect()
    return sa_conn

# %%
def quoted(name, init_quote="[", final_quote="]"):
    sa_engine = gen_default_sa_conn()
    sa_engine.dialect.identifier_preparer.initial_quote = init_quote
    sa_engine.dialect.identifier_preparer.final_quote = final_quote
    quoted_name = sa_engine.dialect.identifier_preparer.quote(name)
    return quoted_name

# %%



