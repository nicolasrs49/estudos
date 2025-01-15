### DATABRICKS ###

### Ler arquivos csv - https://spark.apache.org/docs/latest/sql-data-sources-csv.html
# as "\" no final das linhas é pra quebrar o código em linha nova. Ajuda na leitura e indentação
caminho_csv = "/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.csv"
df = spark.read\ # ler arquivos e armazenando em um dataframe “df”
.option("skipRows", 1)\ # pular uma linha, pq a linha 1 não é o cabeçalho
.option("header", True)\ # agora sim está corretamente na linha do cabeçalho
.option("sep", ";")\ #separador do arquivo
.option("inferSchema", True)\ # tenta inferir o tipo dos dados das colunas
.load(caminho_csv) # o caminho do arquivo a ser lido/carregado

display(df) # exibe os dados de maneira formatada


#  Renomear colunas CSV

df_csv = df_csv.withColumnRenamed(“_c0”, ”ID”).withColumnRenamed(“_c1”, ”NOME”).withColumnRenamed("_c2", "E-mail") # ... e assim por diante

# Compressão de dados CSV
# utilizando o comando 'display' existe uma coluna size que mostra o tamanho do arquivo em bytes

display(dbutils.fs.ls('file:/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.csv'))

# salvar arquivo com compressão
df.write.format("csv").option("compression", "gzip").option('header','true').option('sep',';').mode('overwrite').save("/FileStore/tables/Anac/V_OCORRENCIA_AMPLA-zip")

# ler diretamente do arquivo comprimido

dfnovo = spark.read.option('compression','gzip').option('header','true').option('inferSchema', 'true').option('sep',';').csv("/FileStore/tables/Anac/V_OCORRENCIA_AMPLA-zip")

# reescrevendo usando o comando "Options" para otimizar os parametros
dfnovo = spark.read.options(compression='gzip', header=True, inferSchema=True, sep=';').csv("/FileStore/tables/Anac/V_OCORRENCIA_AMPLA-zip")

# exibir o dataframe
display(dfnovo)

### ARQUIVOS JSON
# https://spark.apache.org/docs/latest/sql-data-sources-json.html

df = spark.read.json("/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")
# exibe o dataframe e renomeia as colunas, caso desejar

# escrever arquivo JSON "json_zip" com compressão gzip
df.write.format('json').option('compression', 'gzip').mode('overwrite').save('/FileStore/tables/Anac/json_zip')

# lendo json compactado 
caminho = "/FileStore/tables/Anac/json_zip/"
df = spark.read.option('compression', 'gzip').json(caminho)
display(df)

### ARQUIVOS PARQUET
# databricks https://docs.databricks.com/pt/query/formats/parquet.html
# spark https://spark.apache.org/docs/latest/sql-data-sources-parquet.html

# Le um arquivo json, armazena em um dataframe parar transformar em Parquet depois
df = spark.read.json("/FileStore/tables/Anac/V_OCORRENCIA_AMPLA.json")

# Grava o arquivo em parquet sem usar Spark
df.write.format('parquet').mode('overwrite').save('/FileStore/tables/Anac/V_OCORRENCIA_AMPLAParquet')

# COMPARAÇÃO
#csv =     9869805  Bytes  9MB
#json=     20436383 Bytes  20MB
#parquet = 3308863  Bytes  3MB

# Lendo o arquivo parquet
dfpq = spark.read.parquet("/FileStore/tables/Anac/V_OCORRENCIA_AMPLAParquet")
display(dfpq)

# compactação maior ainda, comprimindo o parquet em gzip
df.write.format('parquet').option('compression','gzip').mode('overwrite','true').save('/FileStore/tables/Anac/Parquet_zip')

# Lendo arquivo parquet compactado em gzip
dfParquetZip = spark.read.format('parquet').option('compression','gzip').load('/FileStore/tables/Anac/Parquet_zip')

# tempo de leitura do json original vs parquet original
# Parquet foi em torno de 40% mais rápido que o JSON

### PARTICIONAMENTO COM PARQUET
# spark parquet https://spark.apache.org/docs/latest/sql-data-sources-parquet.html

# Lendo arquivo parquet
df = spark.read.parquet("/FileStore/tables/Anac/V_OCORRENCIA_AMPLAParquet")

# Verificando os valores distintos de uma coluna
# o resultado é literalmente um "SELECT DISTINCT" do SQL, 
display(dfpq.select("Classificacao_da_Ocorrência").distinct())

# Particionando o arquivo parquet com uma coluna
dfpq.write.partitionBy("Classificacao_da_Ocorrência").mode('overwrite').parquet("/FileStore/tables/Anac/parquet_particionado")

# Particionando o arquivo parquet com mais colunas
dfpq.write.partitionBy("UF","Classificacao_da_Ocorrência").mode('overwrite').parquet("/FileStore/tables/Anac/parquet_Multiparticionado")