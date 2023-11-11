from classTools import OperationDataBase, FileRetriever, DataExtractor
from classTools import DataProcessor, DataModel

if __name__ == '__main__':
    dBtable = 'dado_diario'
    bDDataDaily = OperationDataBase(dBtable)
    bDDataDaily.setBd(1)
    dM3 = DataModel(bDDataDaily)
    r = FileRetriever('./arquivosModificados')
    r.findFiles()
    files = r.getFoundFiles()
    for currentFile in files:
        dE = DataExtractor()
        dP = DataProcessor()
        print(currentFile)
        dE.dataExtract(currentFile)
        extractData = dE.getExtractData()
        dP.processedData(extractData)
        dataProcessed = dP.getDataProcessed()
        dM3.executeDB(dataProcessed)
        del dE, dP
        print(f'{currentFile} foi salvo no banco de dados.')
        bDDataDaily.closeConnection()
