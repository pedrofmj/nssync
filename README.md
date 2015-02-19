Introdução
==========

O NsSynchronizer é uma ferramenta que sincroniza dados entre o ElasticSearch e o Apache Cassandra.
Para que o sistema funcione, é necessário que o modelo de dados nas duas ferramentas sigam algumas regras.
Os dados tem apenas 1 nível (sem árvores de objetos), e toda tabela/tipo tem um campo id e um campo timestamp.
O campo id é utilizado para ser a chave primária de todas as entidades e possui o formato UUID. No caso do ElasticSearch,
é utilizado este campo como id do objeto, sem atributo adicional para o id. No caso do cassandra, id é uma coluna, mas ambos 
devem ter os mesmo valor para os objetos respectivos. O timestamp é uma marca de tempo (Data e Hora) que representa a última 
modificação do dado. Isto é utilizado para controlar a sincronização.

Forma de Sincronização
----------------------

A ferramenta utiliza as seguintes regras para executar a sincronização:

	1. A ferramenta varre o Cassandra, na busca de novos objetos. Para cada novo objeto encontrado, são executadas 
	   as seguintes regras:
	   
	   (a) Se um documento mais velho com este id é encontrado no ElasticSearch, atualizar o ElasticSearch
	   (b) Se um documento mais novo com este id é encontrado no ElasticSearch, atualizar o Cassandra
	   (c) Se os dois documentos possuem o mesmo timestamp, não fazer nada
	   (d) Se não houver documento no ElasticSearch com este id, indexar um novo documento no ES com estes dados
	   
	2. A ferramenta varre o ElasticSearch, na busca de dados que lá existem, e não existem no Cassandra. No caso,
	   se encontrado, cria-se o objeto no Cassandra, desde que esteja definido e de acordo com o modelo de dados 
	   do Cassandra
 

Suposições e Restrições
-----------------------

Para que o sistema funcione, é necessário que o modelo de dados nas duas ferramentas sigam algumas regras.
Os dados tem apenas 1 nível (sem árvores de objetos), e toda tabela/tipo tem um campo id e um campo timestamp.
O campo id é utilizado para ser a chave primária de todas as entidades e possui o formato UUID. No caso do ElasticSearch,
é utilizado este campo como id do objeto, sem atributo adicional para o id. No caso do cassandra, id é uma coluna, mas ambos 
devem ter os mesmo valor para os objetos respectivos. O timestamp é uma marca de tempo (Data e Hora) que representa a última 
modificação do dado. Isto é utilizado para controlar a sincronização.
Supõe-se também que não são feitas inserções ou atualizações "no passado", ou seja, o timetamp utilizado nas inserções e 
atualizações é exatamente o momento em que estas inserções ou atualizações estão sendo feitas.
O sistema foi desenvolvido conforme o arquivo em anexo Desafio_Técnico_Simbiose_Sincronização_ElasticSearch_Cassandra.pdf,
que se encontra nesta mesma pasta.


Modelo de dados
----------------
O modelo de dados é o utilizado no Cassandra, pois dentre os dois sistemas, é o que necessita de modelagem. Para não ter que 
recriar uma terceira fonte de modelagem, o programa foi feito lendo o modelo de dados definido no Cassandra.

 
Ambiente
--------

O script foi desenvolvido e testado com as seguintes versões das ferramentas:

	- ElasticSearch 1.4.3
	- Apache Cassandra 2.0.12
	
Execução
--------

Para executar a ferramenta, necessário configurar os parâmetros de conexãop no script nssync.py, e executá-lo:

python nssync.py

