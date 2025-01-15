print('### Jornada de dados ###')

# variaveis
idade = 30
nome = 'Nicolas Santos'

# Print - imprimir na tela
# deu erro para printar numero inteiro, então converti para String
print('Olá, meu nome é '+nome+', e tenho '+str(idade)+' anos.')

# Soma
nova_idade = idade +1
print('Nova idade: '+str(nova_idade))

# Lista
lista = [1,2,3,4,5]
print(lista)

for item in lista:
    print(item)

# Métodos
print('Lista invertida: ')
lista_invertida = lista.reverse()
print(lista_invertida) # ta imprimindo "none"