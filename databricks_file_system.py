# SISTEMA INTERNO DE ARQUIVOS DO DATABRICKS

%fs # "linguagem" file system

# Listar arquivos no diretorio "/"
ls /

# cria nova pasta
mkdirs /FileStore/tables/NovaPasta

# Remove pasta vazia
rm /FileStore/tables/NovaPasta

# copiando arquivos
          # Diretorio "de"               # Diretorio "para". Já cria a nova pasta "Bikes2"   
cp /FileStore/tables/Bikes/products.csv    /FileStore/tables/Bikes2/products.csv

# RENOMEANDO o arquivo, utilziando o comando "mv" (move)
mv /FileStore/tables/Bikes2/products.csv /FileStore/tables/Bikes2/NovoNomeTeste.csv

# Removendo pastas com arquivos dentro
rm -r /FileStore/tables/Bikes2 # o parametro -r habilita o modo recursivo, que apaga a pasta com todos os arquivos dentro
