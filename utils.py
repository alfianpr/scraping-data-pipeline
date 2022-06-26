from sqlalchemy import create_engine
from types import SimpleNamespace

def df_to_ps (df, table_name, credential={}):
    cre = SimpleNamespace(**credential)
    conn_string = 'postgresql://{}:{}@{}:{}/{}'.format(cre.username, cre.password, cre.host, cre.port, cre.db_name)
    conn = create_engine(conn_string)
    return df.to_sql(table_name, conn, if_exists= 'append')