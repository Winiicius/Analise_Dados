from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, dense_rank
from pyspark.sql.window import Window


# Inicializa a sessão do Spark
spark = SparkSession.builder.appName("Filmes").getOrCreate()

# Tranforma o arquivo JSON(Resultante da camada "Trusted") em um dataframe
df = spark.read.option("multiline","true").json(r"transformacao\trusted\json\filmes.json")

# Explode a coluna 'generos' para que cada gênero se torne uma linha separada
dataframe_genero_explodido = df.withColumn("genero", explode("generos"))

# Cria uma janela ordenada por 'genero'
window = Window.orderBy("genero")

# Adiciona uma coluna 'id_genero' contendo um número sequencial único para cada gênero
dataframe_generos_com_id = dataframe_genero_explodido.withColumn("id_genero", dense_rank().over(window))

# Cria a dimensão 'dim_genero' contendo os IDs e os nomes dos gêneros, removendo duplicatas
dim_genero = dataframe_generos_com_id.select("id_genero", "genero").distinct()


# Aqui fazemos o mesmo processo com a produtora
# Explode a coluna 'produtoras' para que cada produtora se torne uma linha separada
dataframe_produtora_explodido = df.withColumn("produtora", explode("produtoras"))

# Cria uma janela ordenada por 'produtora'
window = Window.orderBy("produtora")

# Adiciona uma coluna 'id_produtora' contendo um número sequencial único para cada produtora
dataframe_produtoras_com_id = dataframe_produtora_explodido.withColumn("id_produtora", dense_rank().over(window))

# Cria a dimensão 'dim_produtora' contendo os IDs e os nomes das produtoras, removendo duplicatas
dim_produtora = dataframe_produtoras_com_id.select("id_produtora", "produtora").distinct()

# Cria a dimensão 'dim_filme' contendo informações básicas sobre os filmes
dim_filme = df.select("id_filme", "id_imdb", "titulo", "titulo_original", "visao_geral")

# Cria a fato 'fato_filme' contendo métricas relacionadas aos filmes
fato_filme = df.select("id_filme", "media_de_votos", "popularidade", "receita", "tempo_duracao(minutos)", "total_votos")

# Realiza um join entre o dataframe original e a dimensão de gênero para obter os IDs correspondentes
filme_genero = dataframe_genero_explodido.join(dim_genero, dataframe_genero_explodido['genero'] == dim_genero['genero'], 'left_outer').select(dataframe_genero_explodido['id_filme'], dim_genero['id_genero'])

# Realiza um join entre o dataframe original e a dimensão de produtora para obter os IDs correspondentes
filme_produtora = dataframe_produtora_explodido.join(dim_produtora, dataframe_produtora_explodido['produtora'] == dim_produtora['produtora'], 'left_outer').select(dataframe_produtora_explodido['id_filme'], dim_produtora['id_produtora'])

# Mostra os dataframes e dimensões criados
filme_produtora.show()
filme_genero.show()
dim_genero.show()
dim_produtora.show()
dim_filme.show()
fato_filme.show()
