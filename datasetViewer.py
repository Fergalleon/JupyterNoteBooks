
import pyodbc
import array
import pandas as pd
import numpy as np


class datasetViewer:
    def __init__(self, server, database):
        self.server = server
        self.database = database

    def displaySqlResults(self, sql):
        dataWithCols= self.getDataArray( sql)
        data=[dataWithCols[rowIndex] for rowIndex in range(1, len(dataWithCols))]
        return pd.DataFrame(data, columns=dataWithCols[0])  
    
    def getCursor(self):
        server = self.server  #'(localdb)\ProjectsV13'
        database = self.database  #'JupyterDB'

        connectionString = 'DRIVER={SQL Server Native Client 11.0};SERVER='+ server + ';DATABASE=master;Integrated Security=True'
        connection = pyodbc.connect(connectionString)
        cursor = connection.cursor()

        return cursor

    def getDataArray(self, sqlStatement):
        cursor = self.getCursor()
        cursor.execute(sqlStatement)   
        columnNameList = self.getColumnNames(cursor)    
        arr=[]
        # add cols to first row
        arr.append([col for col in columnNameList])

        #add rows
        row = cursor.fetchone()

        while row:
            rowValues = [row[index] for index in range(0, len(columnNameList))]
            rowArray = np.asarray(rowValues)
            arr.append(rowArray)      

            row = cursor.fetchone()

        toReturn = np.asarray(arr)
        return toReturn

    def getColumnNames(self, cursor):
        return [column[0] for column in cursor.description]
