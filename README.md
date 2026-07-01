# Relatório de Pedidos com Pagamento Recusado e Fraude Legítima (PySpark)

Trabalho final da disciplina **Data Engineering Programming** (FIAP).

- **Integrante:** Ayrton Fernandes Tridico, RM372205
- **Professor:** Marcelo Barbosa Pinto

## 1. Objetivo do projeto

A alta gestão da empresa pediu um relatório de pedidos de venda cujo
**pagamento foi recusado** (`status=false`) e que a **avaliação de fraude**
classificou como **legítimo** (`fraude=false`). O relatório:

- contém `id_pedido`, `uf`, `forma_pagamento`, `valor_total` e a data do
  pedido (`data_pedido`);
- compreende apenas pedidos do **ano de 2025**;
- está ordenado por `uf`, `forma_pagamento` e data de criação do pedido;
- é gravado em formato **parquet**.

O projeto foi feito em **PySpark**, com schemas explícitos em todos os
dataframes, orientação a objetos, injeção de dependências, configuração
centralizada, logging, tratamento de erros e testes automatizados.

## 2. Arquitetura e organização do código

```
.
├── src/
│   ├── main.py                      # Aggregation Root (ponto de entrada)
│   ├── config/
│   │   ├── settings.py              # Classe AppConfig (configuração centralizada)
│   │   └── settings.yaml            # Arquivo de configuração externo
│   ├── session/
│   │   └── spark_session.py         # Classe SparkSessionManager
│   ├── io_utils/
│   │   └── data_handler.py          # Classe DataHandler (leitura/escrita, schemas explícitos)
│   ├── processing/
│   │   └── business_logic.py        # Classe LogicaNegocioRelatorio (regras de negócio)
│   └── pipeline/
│       └── pipeline.py              # Classe PipelineRelatorioPedidos (orquestração)
├── tests/
│   ├── conftest.py                  # Fixture de SparkSession compartilhada
│   ├── unit/                        # Testes unitários (lógica de negócio e configuração)
│   └── integration/                 # Teste de integração do pipeline ponta a ponta
├── scripts/
│   └── download_datasets.py         # Download dos datasets públicos de origem
├── data/
│   ├── input/                       # Datasets de origem (baixados localmente, não versionados)
│   └── output/                      # Relatório final em parquet (gerado localmente)
├── pyproject.toml                   # Empacotamento (build) e configuração do pytest
├── requirements.txt                 # Dependências do projeto
├── MANIFEST.in                      # Arquivos extras incluídos no pacote distribuído
└── README.md
```

### 2.1. Responsabilidade de cada componente

| Componente | Pacote | Responsabilidade |
| --- | --- | --- |
| `AppConfig` | `config` | Classe de configuração centralizada. Carrega `settings.yaml` e expõe paths, opções de leitura e regras de negócio configuráveis (ex.: ano de referência). |
| `SparkSessionManager` | `session` | Cria, fornece e finaliza a `SparkSession` da aplicação. |
| `DataHandler` | `io_utils` | Define os schemas explícitos dos dataframes de pedidos e pagamentos, lê os datasets de origem e grava o relatório final em parquet. |
| `LogicaNegocioRelatorio` | `processing` | Contém as regras de negócio: cálculo do valor total, filtro por ano, filtro de pagamentos recusados/legítimos, junção e ordenação do relatório. Usa `logging` e `try/except`, e também configura o logging da aplicação. |
| `PipelineRelatorioPedidos` | `pipeline` | Orquestra a execução: lê os dados via `DataHandler`, aplica as regras da `LogicaNegocioRelatorio` e grava o resultado. |
| `main.py` | raiz | Aggregation Root. Instancia as dependências concretas e as injeta via construtor nas classes que dependem delas. |

### 2.2. Injeção de dependências

`AppConfig`, `SparkSessionManager`, `DataHandler` e `LogicaNegocioRelatorio`
são instanciadas só em `src/main.py` e injetadas via construtor na classe
`PipelineRelatorioPedidos`. Nenhuma classe cria sua própria dependência
internamente, o que mantém o código testável (nos testes, `DataHandler` e
`LogicaNegocioRelatorio` podem ser substituídos por dublês) e desacoplado.

## 3. Como executar

O projeto foi desenvolvido e testado em ambiente Linux (AWS Cloud9). Os
passos abaixo cobrem a instalação e a execução completa nesse ambiente:

```bash
git clone https://github.com/ayrtonftridico/relatorio-pedidos-fraude-pyspark.git
cd relatorio-pedidos-fraude-pyspark
pip install -r requirements.txt
python scripts/download_datasets.py
python src/main.py
pytest
```

Pré-requisitos no ambiente: Python 3.10+, Java JDK 17 ou 21 (vem instalado
por padrão em boa parte das imagens do Cloud9; se faltar, basta instalar com
o gerenciador de pacotes da distribuição) e Git.

## 4. Download dos datasets de origem

Os datasets de pedidos e de pagamentos são públicos e ficam em
`data/input/`. Um script baixa os dois automaticamente:

```bash
python scripts/download_datasets.py
```

Isso clona, dentro de `data/input/`:

- `datasets-csv-pedidos`: https://github.com/infobarbosa/datasets-csv-pedidos
- `dataset-json-pagamentos`: https://github.com/infobarbosa/dataset-json-pagamentos

Também é possível clonar os dois repositórios manualmente para os mesmos
caminhos, caso o script não consiga rodar.

## 5. Executando a aplicação

Com o ambiente ativado, a partir da raiz do repositório:

```bash
python src/main.py
```

A aplicação:

1. Carrega a configuração (`src/config/settings.yaml`);
2. Cria a `SparkSession`;
3. Lê os datasets de pedidos (CSV) e pagamentos (JSON), com schemas explícitos;
4. Aplica as regras de negócio (valor total, filtro do ano de 2025, filtro
   de pagamentos recusados e legítimos, junção e ordenação);
5. Mostra uma amostra do relatório no console;
6. Grava o relatório final em `data/output/relatorio_pedidos_recusados_legitimos`
   (formato parquet);
7. Encerra a `SparkSession`.

Os logs da execução aparecem no console e também ficam gravados no arquivo
`relatorio-pedidos-fraude-pyspark.log`, na raiz do projeto.

### 5.1. Alterando configurações

Caminhos de entrada/saída, opções de leitura, ano de referência e nome da
aplicação Spark ficam todos em `src/config/settings.yaml`. Não há valores
fixos espalhados pelo código.

## 6. Executando os testes automatizados

```bash
pytest
```

A suíte tem testes unitários da lógica de negócio e da configuração
(`tests/unit/`) e um teste de integração do pipeline completo
(`tests/integration/`), totalizando 14 testes. Para rodar só os testes
unitários:

```bash
pytest -m unit
# ou, por diretório:
pytest tests/unit
```

## 7. Empacotamento da aplicação

O projeto pode ser empacotado como um pacote Python instalável:

```bash
pip install build
python -m build
pip install dist/*.whl
```

Depois de instalado, a aplicação pode ser executada pelo comando registrado
em `pyproject.toml`:

```bash
run-relatorio-pedidos
```

## 8. Regras de negócio implementadas

| Regra | Implementação |
| --- | --- |
| Valor total do pedido | `valor_unitario * quantidade` (`LogicaNegocioRelatorio.adicionar_valor_total`) |
| Apenas pedidos de 2025 | `YEAR(data_criacao) = 2025` (`filtrar_pedidos_por_ano`, ano configurável em `settings.yaml`) |
| Pagamento recusado e legítimo | `status = false AND avaliacao_fraude.fraude = false` (`filtrar_pagamentos_recusados_legitimos`) |
| Junção pedidos x pagamentos | Por `id_pedido` (`montar_relatorio`) |
| Ordenação | Por `uf`, `forma_pagamento`, `data_pedido` (`ordenar_relatorio`) |
| Formato de saída | Parquet, modo overwrite (`DataHandler.write_parquet`) |

## 9. Datasets utilizados

- **Pedidos** (CSV, separador `;`, comprimido em gzip):
  https://github.com/infobarbosa/datasets-csv-pedidos
- **Pagamentos** (JSON, comprimido em gzip):
  https://github.com/infobarbosa/dataset-json-pagamentos

## 10. Repositório no GitHub

https://github.com/ayrtonftridico/relatorio-pedidos-fraude-pyspark
