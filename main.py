import os
import csv
import psycopg2
from abc import ABC
from pathlib import Path
from sys import getsizeof
from itertools import groupby
from databaseSettings import CONFIG


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

    def generatorSQLInsert(self, *args, nome_colunas=None,  nome_tabela=None):
        valores = args[0]
        sql = "INSERT INTO %s %s VALUES %s" % (
            nome_tabela, nome_colunas, valores
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

    def __init__(self, tabela) -> None:
        self.tablela = tabela
        self.Bd = DataBase(
            dbname=CONFIG['banco_dados'],
            user=CONFIG['usuario'],
            port=CONFIG['porta'],
            password=CONFIG['senha'],
            host=CONFIG['host']
        )

    def atualizarColuna(self, coluna, condicao, atualizacao):
        sql = self.generatorSQLUpdate(
            atualizacao, nome_tabela=self.tablela,
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
                *args, nome_colunas=coluna, nome_tabela=self.tablela
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
        self.__meanData: list = []

    def processedData(self, listTarget) -> None:
        for groupData in listTarget:
            currentDataList: list = []
            summ: list = [0, 0, 0, 0]
            for data in groupData[1]:
                summ[0] += data[0]
                summ[1] += data[1]
                summ[2] += data[2]
                summ[3] += data[3]

            sizeGroupData = len(groupData[1])
            currentDataList.append(groupData[0])
            currentDataList.append(round((summ[0] / sizeGroupData), 2))
            currentDataList.append(round((summ[1] / sizeGroupData), 2))
            currentDataList.append(round((summ[2] / sizeGroupData), 2))
            currentDataList.append(round((summ[3] / sizeGroupData), 2))
            self.__meanData.append(currentDataList)

    def getMeanData(self) -> list:
        return self.__meanData


if __name__ == '__main__':
    r = FileRetriever('.')
    files = r.getFoundFiles()
    dE = DataExtractor()
    dE.dataExtract(files)
    resultData = dE.getExtractData()
    print(getsizeof(resultData))
    dP = DataProcessor()
    dP.processedData(resultData)
    result = dP.getMeanData()

    print(result)
