# live_1_bilhao_linhas

# ETL - 1 Bilhão de Linhas - Jornada de Dados - Luciano Vasconcelos

Será feito um ETL com 4 engines: Spark, polars, pandas e DuckDB.

A ETL é uma das principais atividades para quem trabalha com dados. Dentro da empresa temos série de sistemas (marketing, logística, erp) e quando queremos fazer uma análise o desafio é conseguir informações que existem em diferentes sistemas. 

Nós temos diferentes ferramentas, mas o processo de ETL sempre vai existir em empresas, e saber fazer isso de ponta a ponta é algo extremamente importante. 

Desafios:

1. Leitura: Como iremos ler 1 bilhão de linhas? São tantos dados que literalmente não conseguimos abrir em ferramentas;
2. Transformações: Informações como min, max, e média.
3. Load: Salvar a informação em algum local para visualizar num dashboard.

Para o ELT, nós conseguimos fazer como se fosse o databricks, uma vez que os dados todos vão lá para dentro e a partir disso nós trabalhamos.

No dbt-core, por exemplo, se usa muito o ELT.

O principal motivo da ETL ao invés da ELT, é que nós temos o store no ELT. Então HOJE em dia é muito comum termos o ELT ao invés do ETL. 

Antigamente era MUITO caro armazenarmos o nosso banco no ELT, então era unicamente feito a partir do ETL. 

# Começo do projeto:

Para começar o projeto, rodamos primeiro de tudo o .env:

```python
poetry init
pyenv local 3.12.1
poetry env use 3.12.1
poetry shell 

```

## Utilizando Python:

```python
def processar_temperaturas(path_do_txt: Path):
    print("Iniciando o processamento do arquivo.")

    start_time = time.time() 

    temperatura_por_station = defaultdict(list)

    with open(file=PATH_DO_TXT, mode='r', encoding='utf-8') as file:
        _reader = reader(file, delimiter= ';')
        for row in _reader:
            nome_da_station, temperatura = str(row[0]), float(row[1])
            temperatura_por_station[nome_da_station].append(temperatura)
```

A utilização do defaultdict(list) cria uma entidade que vai se comportar como um dicionário, porém com valor de lista, por exemplo:

{ ‘cidade’: {45,16,19,22} }

Usando o *defaultDict* conseguimos economizar MUITO mais memória do que apenas em uma implementação de um dicionário comum.

Resultado: Para o Python puro, com um script que possui 100_000_000 (cem milhões de linhas), o resultado demorado foi o seguinte: 86.08 segundos para a leitura, transformação e load destes arquivos.


Uma das características é que Python possui apenas um processo. Ele tem uma tarefa e vai usar o processador para usar nessa tarefa. Ele não divide os serviços, ele é diretamente ligado a como que foi feito o desenvolvimento. Se o desenvolvimento foi algo mais lento (performance baixa) ele vai demorar. 

Por definição, Python é *singlethread* , isso significa: Se temos um computador com 12 núcleos, queremos usar os 12 para trabalhar com todos eles em paralelo. Com python puro, vamos utilizar apenas 1. 

Um bom exemplo da questão de otimização é que podemos usar esse próprio projeto para utilizar o algoritmo de ordenação. 

## Utilizando Pandas:

A vantagem do Pandas para o Python, entra muito na facilidade do arquivo. 

O *Pandas* utiliza a estrutura de *DataFrame,* porém ao invés de ter uma lista com dicionários dentro, ele inspira-se em informações tabulares - colunas e linhas. 

Acaba sendo muito mais fácil de trabalhar e implementar estes dados. 

Pandas usa o numpy por de baixo dos panos para começar a criar arrays. Ao invés de ter grandes listas, nós temos arrays. 

Existem 5 coisas que precisamos prestar atenção para otimizar um código no pandas:

1. Tente carregar menos dados → Faz sentido carregarmos todos os dados? Podemos pegar apenas uma amostra pra fazer os testes. É similar ao que acontece com o select * do SQL. 
2. Use eficientes Datatypes → Tudo que temos em dados temos o valor e o tipo. Quando trabalhamos com as análises/engenharias/ciência queremos conseguir trabalhar com os melhores tipos possíveis. 
Por Default o pandas escolhe o *float64*, e nós iremos alterar para o *float32*.  Nós não precisamos de um dado com tantas casas decimais, nós podemos usar menos casas, e isso faz ser mais rápido. 
Nesse caso, queremos ler a temperatura utilizando o *float32*. 
Para a cidade, podemos normalizar a tabela, tendo a dimensao_cidades.

Por que faremos isso? Pois agora a cidade vai gastar apenas um espaço na memória, e não mais o nome da cidade inteira. 
Para isso, trocaremos para o tipo *Category* que faz exatamente isso. 

Apenas realizar essas mudanças já da uma GRANDE alteração no consumo do dataframe:


Tendo uma redução de 98% na coluna estacao.

É MUITO importante se atentar para esse tipo de situação. 

Uma das desvantagens existentes do pandas é que ele não é feito para grandes volumes de datasets. Uma das possibilidades dai é usar chunking, que é quebrar o dataset em diversas partes para que eles possam se adequar a memoria. 

O Python lê apenas os dados de memória, ele precisa ler TODO o arquivo.

Resultado: Utilizando a separação por chunks e agora fazendo a leitura dos 1 bilhão de linhas, tivemos o seguinte valor: 319 segundos de processamento, um tempo relativamente baixo para a quantidade de linhas.


## Python + Duckdb

Duckdb

- Plano de Execução
- Estrutura de dataset na memória de forma eficiente
- Multiprocessamento

Spark

- Plano de Execução
- Estrutura de dataset na memória de forma eficiente


Como o Duckdb puxa apenas o SQL, então fazemos uma query simples para ele rodar.


## Python + PySpark

O código deve ser feito via databricks (eu tendo saco de fazer toda a instalação do PySpark na máquina local).

O que o DataBricks ganha é para o caso de você ter dinheiro, que dai consegue contratar diversos clusters e etc. Caso contrário, o duckdb ainda ganha.
