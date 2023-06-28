import os
import csv
import psycopg2
from abc import ABC
from pathlib import Path
from itertools import groupby
from databaseSettings import dbCredentials
from datetime import datetime, timedelta
from statistics import mean, median, mode


class DataBase(ABC):
    '''Classe abstrata que fornece os serviços básicos
    para as operações do banco de dados'''
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
    '''Realiza as operações com o PostgreSQL'''

    def __init__(self, table: str) -> None:
        self.__table = table
        self.Bd: DataBase = DataBase

    def setBd(self, dbChoice: int) -> None:
        CONFIG = dbCredentials(dbChoice)
        self.Bd = DataBase(
            dbname=CONFIG['banco_dados'],
            user=CONFIG['usuario'],
            port=CONFIG['porta'],
            password=CONFIG['senha'],
            host=CONFIG['host']
        )

    def updateColumn(self, collumn, condiction, update):
        '''
            Atualiza colunas.
            Parametros: collumn -> Nome da coluna
            condition -> Condição de atualização
            update -> Valor da modificação
        '''
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
        '''
            Insere dados na tabela.
            Parametros:
            *args -> tupla com os valores, em ordem com a coluna
            collumn -> Nome das colunas, na ordem de inserção.
        '''
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
        '''
            Insere dados na tabela usando o comando pysicopg2 mogrify().
            Parametros:
            *args -> tupla com os valores, em ordem com a coluna
            collumn -> Nome das colunas, na ordem de inserção.
        '''
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
        '''
            Fecha a conexão com o banco.
            Deve ser usado ao final das transações.
        '''
        return self.Bd.closeConnection()

    def toExecute(self, sql):
        '''
            Executa um comando SQL avulso.
        '''
        return self.Bd.toExecute(sql)


class FileRetriever:
    '''
        Busca arquivos, manipula caminhos e nomes de arquivos.
    '''
    def __init__(self, pathTarget, extension='.csv') -> None:
        self.__foundFiles: list = []
        self.__pathTarget = pathTarget
        self.__extensionFile = extension

    def findYesterdayFile(self, month, year) -> None:
        '''
            Busca o arquivo cujo o mês está na data de ontem.
            Salva o arquivo no atributo self.__foundFiles
            Retorna -> None
        '''
        try:
            fileName = self.__generatorNameFile(month, year)
            self.__foundFiles.append(self.findOneFile(fileName))
        except Exception as e:
            raise (e.__name__.__class__, e)

    def findFiles(self) -> None:
        '''
            Atributo de classe.
            Busca todos os arquivos cujo a extensão foi definida na pasta.
            Salva o caminho dos arquivos no atributo self.__foundFiles.
            Retorna -> None.
        '''
        for root, _, file_ in os.walk(self.__pathTarget):
            for targetFile in file_:
                if self.__extensionFile in targetFile:
                    self.__foundFiles.append(os.path.join(root, targetFile))

    def findOneFile(self, fileName: str):
        '''
            Busca um arquivo na pasta definida pelo seu nome.
            Retorna o caminho do arquivo se ele existir.
        '''
        for root, _, file_ in os.walk(self.__pathTarget):
            for targetFile in file_:
                if fileName in targetFile:
                    return str(os.path.join(root, targetFile))
        return 'Arquivo não encontrado.'

    def __generatorNameFile(self, month, year):
        '''
            Atributo de classe.
            Gera o nome de um arquivo baseado em seu mês e ano.
            Retorna o nome do arquivo.
        '''
        try:
            nameFile = os.path.join(
                f'{month}_{year}_log{self.__extensionFile}'
            )
            return nameFile
        except Exception as e:
            print(e.__class__.__name__, e)

    def getFoundFiles(self):
        '''
            Retorna o atributo self.__foundFiles.
        '''
        try:
            if self.__foundFiles:
                for files in self.__foundFiles:
                    yield files
            else:
                raise Exception('Arquivos não encontrados')
        except Exception as e:
            print(e)


class DataExtractor:
    '''
        Extrai os dados brutos dos arquivos e agrupa-os por dia.
    '''
    def __init__(self) -> None:
        self.__extractData: list = []

    def dataExtract(self, file: list) -> None:
        '''
            Extrai dados de arquivos .csv.
            Paramtros:
            file -> Nome do arquivo.
            O dados são salvos no atributo self.__extractData
            pelo metodo de classe self.__groupbyDataByDate
        '''
        try:
            PATH_CSV = Path(__file__).parent / file  # type: ignore
            with open(PATH_CSV, 'r', encoding='utf-8') as myCsv:
                reader = csv.reader((line.replace('\0', '') for line in myCsv))
                self.__groupbyDataByDate(reader)
        except (IndexError, Exception) as e:
            raise e

    def extractedDailyData(self, pathFile: str, dateTarget: int) -> list:
        '''Informe o caminho do arquivo e a data da extração. Retorna os dados
        retirados do arquivo'''
        with open(pathFile, 'r', encoding='utf-8') as file:
            dataFile = [x.replace('\0', '') for x in file.readlines()]
            extractDataTarget: list = []
            for data in dataFile[-1::-1]:
                datas = data[:3].strip()
                if datas == '':
                    continue
                if int(datas) > dateTarget:
                    ...
                elif int(datas) == dateTarget:
                    extractDataTarget.append(
                        data.strip().split(',')
                    )
                else:
                    break
        self.__groupbyDataByDate(extractDataTarget)

    def __groupbyDataByDate(self, iterable):
        '''
            Agrupa os dados por data.
            Salva os dados no atributo self.__stractData
        '''
        def __extractKey(listTarget):
            return listTarget[0][:11]

        groups = groupby(iterable, key=__extractKey)
        for date, data in groups:
            self.__extractData.append((date, [
                        (
                            float(value[1]),
                            float(value[2]),
                            float(value[3]),
                            float(value[4])
                        )
                        if
                        value[1] and value[2] and value[3] and value[4] != ''
                        else (0, 0, 0, 0)
                        for value in data
                    ]))

    def getExtractData(self) -> list:
        '''
        Retorna o atributo self.__extractData
        '''
        return self.__extractData


class DataProcessor:
    '''Processa os dados e prapara-os para entrar no banco de dados.'''
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
        '''
        Metodo de classe que formata datas no formato do Banco de Dados.
        Retorna uma string com a data formatada.
        '''
        if dateOld[3:6] in self.__numbersOfMonth:
            for k, v in self.__numbersOfMonth.items():
                if k == dateOld[3:6]:
                    nD = dateOld.replace(k, str(v))
                    if int(nD[3:5].strip()) > 9:
                        dTStr = f'{nD[5:]}/{nD[3:5]}/{nD[:2]} 00:00:00'.strip()
                        nD = datetime.strptime(dTStr, '%Y/%m/%d %H:%M:%S')
                    else:
                        dTStr = f'{nD[5:]}/{nD[3]}/{nD[:2]} 00:00:00'.strip()
                        nD = datetime.strptime(dTStr, '%Y/%m/%d %H:%M:%S')
        else:
            for k, v in self.__numbersOfMonthEnglish.items():
                if k == dateOld[3:6]:
                    nD = dateOld.replace(k, str(v))
                    if int(nD[3:5].strip()) > 9:
                        dTStr = f'{nD[5:]}/{nD[3:5]}/{nD[:2]} 00:00:00'.strip()
                        nD = datetime.strptime(dTStr, '%Y/%m/%d %H:%M:%S')
                    else:
                        dTStr = f'{nD[5:]}/{nD[3]}/{nD[:2]} 00:00:00'.strip()
                        nD = datetime.strptime(dTStr, '%Y/%m/%d %H:%M:%S')

        newDate = nD.strftime('%Y/%m/%d %H:%M:%S')
        return newDate

    def processedData(self, listTarget) -> None:
        '''
        Processa a lista com os dados agrupados por data.
        Os dados são salvos no atributo self.__dataProcessed.
        '''
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
                if 0 < data[0] <= 100:
                    humidity.append(data[0])
                if 0 < data[1] <= 1000:
                    press.append(data[1])
                if 0 < data[2] < 50:
                    tempIndoor.append(data[2])
                if 0 < data[3] < 50:
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
        '''
        Retorna o atributo self.__dataProcessed
        '''
        return self.__dataProcessed


class ConverterMonths:
    '''
        Converte os números dos meses em suas abreviações.
    '''
    def __init__(self) -> None:
        self.__numbersOfMonth = {
            '01': 'jan',
            '02': 'fev',
            '03': 'mar',
            '04': 'abr',
            '05': 'mai',
            '06': 'jun',
            '07': 'jul',
            '08': 'ago',
            '09': 'set',
            '10': 'out',
            '11': 'nov',
            '12': 'dez'
        }
        self.__numbersOfMonthEnglish = {
            '01': 'jan',
            '02': 'feb',
            '03': 'mar',
            '04': 'apr',
            '05': 'may',
            '06': 'jun',
            '07': 'jul',
            '08': 'aug',
            '09': 'sep',
            '10': 'oct',
            '11': 'nov',
            '12': 'dec'
        }

    def getMonths(self, numberOfMont: str) -> str:
        '''
            Informe uma string contendo o número correspondente ao mês.
            Retorna a abreviação do mês.
        '''
        if numberOfMont in self.__numbersOfMonth:
            return self.__numbersOfMonth[numberOfMont]


class DailyDate:
    '''Manipula datas.'''
    def __init__(self) -> None:
        self.__todayDate: datetime = datetime.now()

    def yesterdayDate(self) -> datetime:
        '''Retorna a data de ontem.'''
        return self.__todayDate - timedelta(1)

    def getTodayDate(self) -> datetime:
        '''Retorna o atributo __todayDate, contendo a data atual.'''
        return self.__todayDate

    def extractDay(self, date: datetime) -> str:
        '''Retorna o dia da data informada.'''
        dd = datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S.%f')
        extratcDay = dd.strftime('%d')
        return extratcDay

    def extractMonth(self, date: datetime) -> str:
        '''Retorna o mês da data informada.'''
        dm = datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S.%f')
        extratcMonth = dm.strftime('%m')
        return extratcMonth

    def extractYear(self, date: datetime) -> str:
        '''Retorna o ano da data informada.'''
        dt = datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S.%f')
        extratcYear = dt.strftime('%Y')
        return extratcYear


class DataModel:
    '''Modelo dos dados do banco'''
    def __init__(self, dB: OperationDataBase) -> None:
        self.DBInstance = dB

    def executeDB(self, iterable: dict) -> None:
        '''
            Insere os dados extraidos no modelo do BD.
            Retorna -> None
        '''
        for dataDays in iterable:
            try:
                self.DBInstance.toExecute('SET datestyle to ymd')
                self.DBInstance.insertCollumn(
                    (dataDays['date'],
                        dataDays['umidity']['mean'],
                        dataDays['umidity']['minimum'],
                        dataDays['umidity']['maximum'],
                        dataDays['umidity']['median'],
                        dataDays['umidity']['mode'],
                        dataDays['press']['mean'],
                        dataDays['press']['minimum'],
                        dataDays['press']['maximum'],
                        dataDays['press']['median'],
                        dataDays['press']['mode'],
                        dataDays['tempIndoor']['mean'],
                        dataDays['tempIndoor']['minimum'],
                        dataDays['tempIndoor']['maximum'],
                        dataDays['tempIndoor']['median'],
                        dataDays['tempIndoor']['mode'],
                        dataDays['tempOutdoor']['mean'],
                        dataDays['tempOutdoor']['minimum'],
                        dataDays['tempOutdoor']['maximum'],
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
            except Exception as e:
                print(e.__class__.__name__, e)
                continue


if __name__ == '__main__':
    m = ConverterMonths()
    print(m.getMonths('05'))
