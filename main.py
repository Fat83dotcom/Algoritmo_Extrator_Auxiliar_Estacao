import os
import csv
import psycopg2
from abc import ABC
from pathlib import Path
from statistics import mean, median, mode
from itertools import groupby
from databaseSettings import CONFIG
from datetime import datetime


class DataBase(ABC):
    def __init__(
            self, host='', port='', dbname='', user='', password=''
            ) -> None:
        self.con = psycopg2.connect(
            host=host, port=port, dbname=dbname, user=user, password=password)
        self.cursor = self.con.cursor()

    def closeConnection(self):
        self.con.close()

    def toExecute(self, sql):
        self.cursor.execute(sql)

    def toSend(self):
        self.con.commit()

    def toAbort(self):
        self.con.rollback()

    def seekData(self):
        return self.cursor.fetchall()

    def seekOneData(self):
        return self.cursor.fetchone()

    def seekInterval(self, intervalo):
        return self.cursor.fetchmany(intervalo)

    def generatorSQLInsert(self, *args, colunm_names=None,  table_name=None):
        values = args[0]
        if len(values) == 1:
            values = str(values).replace(',', '')
        sql = "INSERT INTO %s %s VALUES %s" % (
            table_name, colunm_names, values
        )
        return sql

    def generatorSQLUpdate(
            self, *args, nome_colunas=None, nome_tabela=None, condicao=None
            ):
        valores = args[0]
        sql = "UPDATE %s SET %s='%s' WHERE %s" % (
            nome_tabela, nome_colunas, valores, condicao
        )
        return sql


class OperationDataBase(DataBase):

    def __init__(self, table: str) -> None:
        self.__table = table
        self.Bd = DataBase(
            dbname=CONFIG['banco_dados'],
            user=CONFIG['usuario'],
            port=CONFIG['porta'],
            password=CONFIG['senha'],
            host=CONFIG['host']
        )

    def atualizarColuna(self, coluna, condicao, atualizacao):
        sql = self.generatorSQLUpdate(
            atualizacao, nome_tabela=self.__table,
            nome_colunas=coluna, condicao=condicao)
        try:
            self.Bd.toExecute(sql)
            self.Bd.toSend()
        except Exception as e:
            self.Bd.toAbort()
            raise e

    def inserirColunas(self, *args, coluna):
        try:
            sql = self.generatorSQLInsert(
                *args, colunm_names=coluna, table_name=self.__table
            )
            self.Bd.toExecute(sql)
            self.Bd.toSend()
        except Exception as e:
            self.Bd.toAbort()
            raise e

    def closeConnection(self):
        return self.Bd.closeConnection()


class FileRetriever:
    def __init__(self, path) -> None:
        self.__foundFiles: list = []
        self.__path = path

    def __fileHunter(self) -> None:
        for _, _, file_ in os.walk(self.__path):
            for targetFile in file_:
                if '.csv' in targetFile:
                    self.__foundFiles.append(targetFile)

    def getFoundFiles(self) -> list:
        try:
            self.__fileHunter()
            if self.__foundFiles:
                for files in self.__foundFiles:
                    yield files
            else:
                raise "Lista de arquivos vazia"
        except Exception as e:
            print(e)


class DataExtractor:
    def __init__(self) -> None:
        self.__extractData: list = []

    def dataExtract(self, files: list) -> None:
        def __extractKey(listTarget):
            return listTarget[0][:11]

        for fileName in files:
            PATH_CSV = Path(__file__).parent / fileName
            with open(PATH_CSV, 'r', encoding='utf-8') as myCsv:
                reader = csv.reader((line.replace('\0', '') for line in myCsv))
                groups = groupby(reader, key=__extractKey)
                for date, data in groups:
                    self.__extractData.append((date, [
                        (
                            float(values[1]),
                            float(values[2]),
                            float(values[3]),
                            float(values[4])
                        ) for values in data
                    ]))

    def getExtractData(self) -> list:
        return self.__extractData


class DataProcessor:
    def __init__(self) -> None:
        self.__dataProcessed: list = []
        self.__numbersOfMonth = {
            'jan': 1,
            'fev': 2,
            'mar': 3,
            'abr': 4,
            'mai': 5,
            'jun': 6,
            'jul': 7,
            'ago': 8,
            'set': 9,
            'out': 10,
            'nov': 11,
            'dez': 12
        }

    def processedData(self, listTarget) -> None:
        for groupData in listTarget:
            currentData: dict = {
                'date': '',
                'umidity': {
                    'minimum': float,
                    'maximum': float,
                    'mean': float,
                    'median': float,
                    'mode': float
                },
                'press': {
                    'minimum': float,
                    'maximum': float,
                    'mean': float,
                    'median': float,
                    'mode': float
                },
                'tempIndoor': {
                    'minimum': float,
                    'maximum': float,
                    'mean': float,
                    'median': float,
                    'mode': float
                },
                'tempOutdoor': {
                    'minimum': float,
                    'maximum': float,
                    'mean': float,
                    'median': float,
                    'mode': float
                }
            }
            humidity: list = []
            press: list = []
            tempIndoor: list = []
            tempOutdoor: list = []

            for data in groupData[1]:
                humidity.append(data[0])
                press.append(data[1])
                tempIndoor.append(data[2])
                tempOutdoor.append(data[3])

            currentData.update({'date': groupData[0]})
            currentData.update({'umidity': {
                    'minimum': min(humidity),
                    'maximum': max(humidity),
                    'mean': round(mean(humidity), 2),
                    'median': median(humidity),
                    'mode': mode(humidity)
                }})
            currentData.update({'press': {
                    'minimum': min(press),
                    'maximum': max(press),
                    'mean': round(mean(press), 2),
                    'median': median(press),
                    'mode': mode(press)
                }})
            currentData.update({'tempIndoor': {
                    'minimum': min(tempIndoor),
                    'maximum': max(tempIndoor),
                    'mean': round(mean(tempIndoor), 2),
                    'median': median(tempIndoor),
                    'mode': mode(tempIndoor)
                }})
            currentData.update({'tempOutdoor': {
                    'minimum': min(tempOutdoor),
                    'maximum': max(tempOutdoor),
                    'mean': round(mean(tempOutdoor), 2),
                    'median': median(tempOutdoor),
                    'mode': mode(tempOutdoor)
                }})
            self.__dataProcessed.append(currentData)

    def getMeanData(self) -> list:
        return self.__dataProcessed


if __name__ == '__main__':
    bDMonthYear = OperationDataBase('mes_ano')
    bDDataDaily = OperationDataBase('dado_diario')
    r = FileRetriever('.')
    files = r.getFoundFiles()
    dE = DataExtractor()
    dE.dataExtract(files)
    resultData = dE.getExtractData()
    dP = DataProcessor()
    dP.processedData(resultData)
    result = dP.getMeanData()

    for dataDays in result:
        for k, v in dataDays.items():
            print(k, v)
            if k == 'date':
                print(v[7:], v[3:6], v[:3])
                # date = datetime(v[7:], v[3:6], v[:3])
                # print(date)

    # for i in files:
    #     print(i[:8])
    #     bDMonthYear.inserirColunas((i[:8],), coluna='(mes_ano)')

    bDMonthYear.closeConnection()
    bDDataDaily.closeConnection()
