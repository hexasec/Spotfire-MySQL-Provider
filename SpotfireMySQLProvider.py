from System import *
from System.Collections.Generic import List
from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Data.Import import *

class SpotfireMySQLProvider:
    __server = ""
    __database = ""
    __uid = ""
    __pwd = ""
    __port = 0
    __settings = ""
    __dotnet_mysql_module = "MySql.Data.MySqlClient"
    __tmp_datatable_name = "smp_tmp"
    
     ########
    # MAIN #
   ########
    
    def __init__(self, _server = "", _database = "", _uid = "", _pwd = "", _port=3306):
        self.__server = _server
        self.__database = _database
        self.__uid = _uid
        self.__pwd = _pwd
        self.__port = _port
        self.setSettings()

    def setServer(self, _server):
        self.__server = _server

    def setDatabase(self, _database):
        self.__database = _database

    def setUID(self, _uid):
        self.__uid = _uid

    def setPassword(self, _pwd):
        self.__pwd = _pwd

    def setPort(self, _port):
        self.__port = _port

    def setSettings(self):
        self.__settings = "Server=" + self.__server + ";Database=" + self.__database + ";"
        if self.__port != 0 or self.__port != 3306:
            self.__settings += "Port=" + self.__port.ToString() + ";"
        self.__settings += "Uid=" + self.__uid + ";Pwd=" + self.__pwd

    def query(self, query):
        dbsettings = DatabaseDataSourceSettings(self.__dotnet_mysql_module, self.__settings, query)
        dbdatasource = DatabaseDataSource(dbsettings)
        datatable = Document.Data.Tables.Add(self.__tmp_datatable_name, dbdatasource)
        Document.Data.Tables.Remove(datatable)

    def select(self, tablename, custom_query = None):
        query = "SELECT * FROM " + tablename
        if custom_query != None:
            query = custom_query
        dbsettings = DatabaseDataSourceSettings(self.__dotnet_mysql_module, self.__settings, query)
        dbdatasource = DatabaseDataSource(dbsettings)
        datatable = Document.Data.Tables.Add(self.__tmp_datatable_name, dbdatasource)
        data = {}
        for col in datatable.Columns:
            data[col.Name] = self.__getColumnRows(self.__tmp_datatable_name, col.Name, col.Properties.DataType)
        Document.Data.Tables.Remove(datatable)
        return data

     ########
    # MISC #
   ########

    def __getColumnRows(self, tablename, colname, coltype):
        types = {DataType.String: str, DataType.Integer: int, DataType.LongInteger: long, DataType.Real: float, DataType.SingleReal: float, DataType.Currency: long, DataType.Date: DateTime, DataType.Time: DateTime, DataType.DateTime: DateTime, DataType.TimeSpan: TimeSpan, DataType.Boolean: bool, DataType.Binary: str}
        if types.get(coltype) != None:
            list = List[types.get(coltype)]()
            col = Document.Data.Tables[tablename].Columns[colname]
            cursor = DataValueCursor.Create[types.get(coltype)](col)
            return self.__col2list(tablename, cursor, list)
        return None
    
    def __col2list(self, tablename, cursor, list):
        for row in Document.Data.Tables[tablename].GetRows(cursor):
            list.Add(cursor.CurrentValue)
        return list
