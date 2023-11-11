import os
import csv
from main import FileRetriever

try:
    pathFolderFiles = '/home/fernando/Área de Trabalho/Projeto_Estacao/Algoritmo_Extrator_Auxiliar_Estacao/csv_2023'
    r = FileRetriever(pathFolderFiles)
    r.findFiles()
    path = r.getFoundFiles()

    for fileTarget in path:
        nameFileTarget = fileTarget
        with open(fileTarget, 'r', encoding='utf-8') as readFile:
            modifiedFile = os.path.split(fileTarget)[1]
            print(modifiedFile)
            modifiedFile = f'{modifiedFile[:9]}Mod{modifiedFile[9:]}'
            folderModFileSave = os.path.join(
                './arquivosModificados', modifiedFile
            )
            print(nameFileTarget)
            print(folderModFileSave)
            reader = csv.reader((line.replace('\0', '') for line in readFile))
            with open(folderModFileSave, 'a', encoding='utf-8') as writeFile:
                write = csv.writer(writeFile)
                for line in reader:
                    lineSplit = line[0].split(' ')
                    lineSplit[1] = lineSplit[1].lower()
                    line[0] = ' '.join(lineSplit)
                    write.writerow(line)
except Exception as e:
    print(e.__class__.__name__, e)
