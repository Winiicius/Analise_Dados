from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json

spark = SparkSession.builder.appName("Filmes").getOrCreate()

# Lista dos nomes dos arquivos JSON
nomes_arquivos = ["filmes(1).json", "filmes(2).json", "filmes(3).json", "filmes(4).json", "filmes(5).json", "filmes(6).json"]

# # Lista para armazenar os DataFrames lidos de cada arquivo
dataframes = []

# Leitura dos arquivos JSON e armazenamento em uma lista de DataFrames
for nome_arquivo in nomes_arquivos:
    df = spark.read.option("multiline","true").json(f"extracao/json/{nome_arquivo}")
    dataframes.append(df)

# Concatenação de todos os DataFrames em um único DataFrame
df_final = dataframes[0]
for df in dataframes[1:]:
    df_final = df_final.union(df)

# Remoção das linhas com valores nulos em 'id_imdb', 'receita' ou 'total_votos'
df_sem_nulos = df_final.filter((col("id_imdb") != "null") & (col("receita")!=0) & (col("total_votos")!= 0))


###################################################
# Processo para salvar arquivo
# Converte o DataFrame em uma lista de strings JSON
lista_json = df_sem_nulos.toJSON().collect()

# Converte cada string JSON em um dicionário
lista_dict = [json.loads(x) for x in lista_json]

# Salva a lista de dicionários em um arquivo JSON
with open(r'transformacao\trusted\json\filmes.json', 'w', encoding="utf-8") as f:
    json.dump(lista_dict, f, indent=4, ensure_ascii=False)

