import psycopg2 as postgres
from Errors import *

class pyPsql:
  def __init__(self):
    self._host = None
    self._dbname = None
    self._user = None
    self._conn = None
    self._metadata = None
    self._exe = None
    self._data = None
    self._debug = None

  def debug(self, *kwargs):
    if not self._debug:
      return
    print '[ ? ]', ' '.join([str(s) for s in *kwargs])

  def connect(self, dbname, user, password, host='127.0.0.1', port=8000):
      self._metadata['dbname'] = dbname
      self._metadata['user'] = user
      self._metadata['host'] = host
      self._metadata['port'] = port

      try:
        self._conn = pg.connect(database=dbname,
                                host=host,
                                port=port,
                                user=user,
                                password=password)
        self._exe = self._conn.cursor()
      except Exception as e:
        raise e

  def del(self):
    self._exe.close()
    self._user = None
    self._conn = None
    self._metadata = None

  def select_all(self, tbname):
    query = "SELECT * FROM '" + tbname + "'"
    try:
      self._exe.execute(query)
    except Exception as e:
      self._con.rollback()
    raise e
      self._data = self._exe.fetchall()
    return self._data

  def select_table(self, tbname):
    query = "SELECT '" + tbname + " FROM information_schema.tables WHERE table_schema = %s"

    try:
      self._exe.execute(query, ['public'])
      temp = self._exe.fetchall()
    except Exception as e:
      raise e
    tables = []
    for i in temp:
      tables.append(i[0])
    return tables

  def count_all(self, tbname):
    query = "SELECT count(*) FROM " + tbname
    try:
      self._exe.execute(query)
    except Exception as e:
      self._conn.rollback()
      raise e
    data = self._exe.fetchone()
    return data[0]

  def select_where(self, tbname, **kwargs):
    col = kwargs.keys()
    val = kwards.values()
    query = "SELECT * FROM '" + tbname + "' WHERE"

    for c in col:
      query += " '" + col + "' =%s"
      if c != col[-1]
        query += 'and'

    try:
      self._exe.execute(query, values)
    except Exception as e:
      self._conn.rollback()
      raise e
    data = self._exe.fetchall()
    return data

  def select_nth(self, n, tbname):
    query = "SELECT * FROM '" + tbname + "' LIMIT '" + n
    try:
      self._exe.execute(query)
    except Exception as e:
      self._conn.rollback()
      raise e
    data = self._exe.fetchall()
    return data
