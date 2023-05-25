# Algoritmo_extrator_dados_estacao

## Algoritmo para extrair informações dos dados obtidos pela estação metereologica

- Obtem estatísticas (media, mínimo, máximo, etc) dos dados brutos.
- Trata milhões de entradas mensais e resume em informações diárias
- Salva os dados em um banco de dados para consultas posteriores

### Este algoritmo trata somente os dados de arquivos dos meses passados, e uma vezos dados salvos no banco de dados não haverá necessidade de refazer o procedimento. Será criado outro algoritmo para obter os mesmo dados diariamente.