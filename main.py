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

    def toExecuteMogrify(self, sql):
        self.cursor.mogrify(sql)

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
            self, *args, collumn_name=None, table_name=None, condiction=None
            ):
        valores = args[0]
        sql = "UPDATE %s SET %s='%s' WHERE %s" % (
            table_name, collumn_name, valores, condiction
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

    def updateColumn(self, collumn, condiction, update):
        sql = self.generatorSQLUpdate(
            update, table_name=self.__table,
            collumn_name=collumn, condiction=condiction)
        try:
            self.Bd.toExecute(sql)
            self.Bd.toSend()
        except Exception as e:
            self.Bd.toAbort()
            raise e

    def insertCollumn(self, *args, collumn):
        try:
            sql = self.generatorSQLInsert(
                *args, colunm_names=collumn, table_name=self.__table
            )
            self.Bd.toExecute(sql)
            self.Bd.toSend()
        except Exception as e:
            self.Bd.toAbort()
            raise e

    def insertCollumnMogrify(self, *args, collumn):
        try:
            sql = self.generatorSQLInsert(
                *args, colunm_names=collumn, table_name=self.__table
            )
            self.Bd.toExecuteMogrify(sql)
            self.Bd.toSend()
        except Exception as e:
            self.Bd.toAbort()
            raise e

    def closeConnection(self):
        return self.Bd.closeConnection()

    def toExecute(self, sql):
        return self.Bd.toExecute(sql)


class FileRetriever:
    def __init__(self, path) -> None:
        self.__foundFiles: list = []
        self.__path = path

    def __fileHunter(self) -> None:
        for root, _, file_ in os.walk(self.__path):
            for targetFile in file_:
                if '.csv' in targetFile:
                    self.__foundFiles.append(os.path.join(root, targetFile))

    def oneFileHunter(self, fileName: str) -> str:
        for root, _, file_ in os.walk(self.__path):
            for targetFile in file_:
                if fileName in targetFile:
                    return os.path.join(root, targetFile)
                else:
                    return 'Arquivo não encontrado.'

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

    def dataExtract(self, file: list) -> None:
        try:
            def __extractKey(listTarget):
                return listTarget[0][:11]

            PATH_CSV = Path(__file__).parent / file
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
                        )
                        if
                        values[1] and values[2] and values[3] and values[4] != ''
                        else (0, 0, 0, 0)
                        for values in data
                    ]))
        except (IndexError, Exception) as e:
            raise e

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
        self.__numbersOfMonthEnglish = {
            'jan': 1,
            'feb': 2,
            'mar': 3,
            'apr': 4,
            'may': 5,
            'jun': 6,
            'jul': 7,
            'aug': 8,
            'sep': 9,
            'oct': 10,
            'nov': 11,
            'dec': 12
        }

    def __dateTransformer(self, dateOld: str) -> str:
        if dateOld[3:6] in self.__numbersOfMonth:
            for k, v in self.__numbersOfMonth.items():
                if k == dateOld[3:6]:
                    nD = dateOld.replace(k, str(v))
                    if int(nD[3:5].strip()) > 9:
                        dStr = f'{nD[5:]}/{nD[3:5]}/{nD[:2]} 00:00:00'.strip()
                        nD = datetime.strptime(dStr, '%Y/%m/%d %H:%M:%S')
                    else:
                        dStr = f'{nD[5:]}/{nD[3]}/{nD[:2]} 00:00:00'.strip()
                        nD = datetime.strptime(dStr, '%Y/%m/%d %H:%M:%S')
        else:
            for k, v in self.__numbersOfMonthEnglish.items():
                if k == dateOld[3:6]:
                    nD = dateOld.replace(k, str(v))
                    if int(nD[3:5].strip()) > 9:
                        dStr = f'{nD[5:]}/{nD[3:5]}/{nD[:2]} 00:00:00'.strip()
                        nD = datetime.strptime(dStr, '%Y/%m/%d %H:%M:%S')
                    else:
                        dStr = f'{nD[5:]}/{nD[3]}/{nD[:2]} 00:00:00'.strip()
                        nD = datetime.strptime(dStr, '%Y/%m/%d %H:%M:%S')

        newDate = nD.strftime('%Y/%m/%d %H:%M:%S')
        return newDate

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

            currentData.update({'date': self.__dateTransformer(groupData[0])})
            currentData.update({'umidity': {
                    'minimum': round(min(humidity), 2),
                    'maximum': round(max(humidity), 2),
                    'mean': round(mean(humidity), 2),
                    'median': round(median(humidity), 2),
                    'mode': round(mode(humidity), 2)
                }})
            currentData.update({'press': {
                    'minimum': round(min(press), 2),
                    'maximum': round(max(press), 2),
                    'mean': round(mean(press), 2),
                    'median': round(median(press), 2),
                    'mode': round(mode(press), 2)
                }})
            currentData.update({'tempIndoor': {
                    'minimum': round(min(tempIndoor), 2),
                    'maximum': round(max(tempIndoor), 2),
                    'mean': round(mean(tempIndoor), 2),
                    'median': round(median(tempIndoor), 2),
                    'mode': round(mode(tempIndoor), 2)
                }})
            currentData.update({'tempOutdoor': {
                    'minimum': round(min(tempOutdoor), 2),
                    'maximum': round(max(tempOutdoor), 2),
                    'mean': round(mean(tempOutdoor), 2),
                    'median': round(median(tempOutdoor), 2),
                    'mode': round(mode(tempOutdoor), 2)
                }})
            self.__dataProcessed.append(currentData)

    def getDataProcessed(self) -> list:
        return self.__dataProcessed


if __name__ == '__main__':
    try:
        bDDataDaily = OperationDataBase('dado_diario')
        r = FileRetriever('./arquivosModificados')
        files = r.getFoundFiles()
        for currentFile in files:
            dE = DataExtractor()
            dP = DataProcessor()
            print(currentFile)
            dE.dataExtract(currentFile)
            extractData = dE.getExtractData()
            dP.processedData(extractData)
            dataProcessed = dP.getDataProcessed()

            for dataDays in dataProcessed:
                bDDataDaily.insertCollumn(
                    (dataDays['date'],
                        dataDays['umidity']['minimum'],
                        dataDays['umidity']['maximum'],
                        dataDays['umidity']['mean'],
                        dataDays['umidity']['median'],
                        dataDays['umidity']['mode'],
                        dataDays['press']['minimum'],
                        dataDays['press']['maximum'],
                        dataDays['press']['mean'],
                        dataDays['press']['median'],
                        dataDays['press']['mode'],
                        dataDays['tempIndoor']['minimum'],
                        dataDays['tempIndoor']['maximum'],
                        dataDays['tempIndoor']['mean'],
                        dataDays['tempIndoor']['median'],
                        dataDays['tempIndoor']['mode'],
                        dataDays['tempOutdoor']['minimum'],
                        dataDays['tempOutdoor']['maximum'],
                        dataDays['tempOutdoor']['mean'],
                        dataDays['tempOutdoor']['median'],
                        dataDays['tempOutdoor']['mode']), collumn='(dia, \
                        media_umidade, \
                        minimo_umidade, \
                        maximo_umidade, \
                        mediana_umidade, \
                        moda_umidade, \
                        media_pressao, \
                        minimo_pressao, \
                        maximo_pressao, \
                        mediana_pressao, \
                        moda_pressao, \
                        media_temp_int, \
                        minimo_temp_int, \
                        maximo_temp_int, \
                        mediana_temp_int, \
                        moda_temp_int, \
                        media_temp_ext, \
                        minimo_temp_ext, \
                        maximo_temp_ext, \
                        mediana_temp_ext, \
                        moda_temp_ext\
                    )')
            del dE, dP
            print(f'{currentFile} foi salvo no banco de dados.')
    except Exception as e:
        print(e.__class__.__name__, e)

    bDDataDaily.closeConnection()
