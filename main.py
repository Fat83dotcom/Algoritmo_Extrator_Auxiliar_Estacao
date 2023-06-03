from classTools import OperationDataBase, FileRetriever, DataExtractor
from classTools import DataProcessor, DataModel

if __name__ == '__main__':
    try:
        bDDataDaily = OperationDataBase('dado_diario')
        bDDataDaily.setBd(1)
        bDDataDaily2 = OperationDataBase('dado_diario')
        bDDataDaily2.setBd(2)
        dM = DataModel(bDDataDaily)
        dM2 = DataModel(bDDataDaily2)
        r = FileRetriever('./arquivosModificados')
        r.findFiles()
        files = r.getFoundFiles()
        for currentFile in files:
            try:
                dE = DataExtractor()
                dP = DataProcessor()
                print(currentFile)
                dE.dataExtract(currentFile)
                extractData = dE.getExtractData()
                dP.processedData(extractData)
                dataProcessed = dP.getDataProcessed()
                dM.executeDB(dataProcessed)
                dM2.executeDB(dataProcessed)
                del dE, dP
                print(f'{currentFile} foi salvo no banco de dados.')
            except Exception as e:
                print(e.__class__.__name__, e)
                continue

        bDDataDaily.closeConnection()
        bDDataDaily2.closeConnection()
    except Exception as e:
        print(e.__class__.__name__, e)
