from classTools import OperationDataBase, FileRetriever, DataExtractor
from classTools import DataProcessor

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
                try:
                    bDDataDaily.toExecute('SET datestyle to ymd')
                    bDDataDaily.insertCollumn(
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
            del dE, dP
            print(f'{currentFile} foi salvo no banco de dados.')
        bDDataDaily.closeConnection()
    except Exception as e:
        print(e.__class__.__name__, e)
