from classTools import OperationDataBase, FileRetriever, DataExtractor
from classTools import DataProcessor, DataModel

if __name__ == '__main__':
    try:
        dBtable = 'dado_diario'
        # bDDataDaily = OperationDataBase(dBtable)
        # bDDataDaily.setBd(1)
        # bDDataDaily2 = OperationDataBase(dBtable)
        # bDDataDaily2.setBd(2)
        bDDataDaily3 = OperationDataBase(dBtable)
        bDDataDaily3.setBd(3)
        # dM = DataModel(bDDataDaily)
        # dM2 = DataModel(bDDataDaily2)
        dM3 = DataModel(bDDataDaily3)
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
                # dM.executeDB(dataProcessed)
                # dM2.executeDB(dataProcessed)
                dM3.executeDB(dataProcessed)
                del dE, dP
                print(f'{currentFile} foi salvo no banco de dados.')
            except Exception as e:
                print(e.__class__.__name__, e)
                continue

        # bDDataDaily.closeConnection()
        # bDDataDaily2.closeConnection()
        bDDataDaily3.closeConnection()
    except Exception as e:
        print(e.__class__.__name__, e)
