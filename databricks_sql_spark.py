query = '''
select 
    o.order_id
    ,i.quantity              as Quantidade
    ,p.product_name          as Produto
    ,b.brand_name            as Marca
    ,p.model_year            as Modelo
    ,ct.category_name        as Categoria
    ,i.list_price            as Valor
    ,o.order_date            as DtCompra
    ,c.first_name            as Cliente
    ,c.email                 as EmailCliente
    ,c.city                  as Cidade
    ,c.state                 as Estado
    ,s.store_name            as LojaCompra
    ,s.email                 as EmailLoja
from order_items as i 
left join  orders as  o on i.order_id = o.order_id
left join clientes    C  on o.customer_id = c.customer_id
left join produtos    P  on i.product_id = p.product_id
left join stores      S  on o.store_id = s.store_id
left join brands      B  on p.brand_id = b.brand_id
left join categories  CT on p.category_id = ct.category_id
'''
# Executando a query com spark e salvando em uma variável
resultado = spark.sql(query)

# Salvar como uma tabela utilizando Delta do Spark            'nome_banco'.'nome_tabela'
# Tabelas Delta vai criando uma linhagem dos dados, pois armazena todo o histórico de modificões no Hive Metastore, podendo ser recuperado qdo desejado
resultado.write.format('delta').mode('overwrite').saveAsTable('Bikescsv.ConsultaCompleta')

