"""Pacote de leitura e escrita de dados (I/O).

Concentra toda a interação com o sistema de arquivos (leitura dos datasets de
origem e escrita do relatório final), incluindo a definição explícita dos
schemas de cada dataframe.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.utils import AnalysisException

logger = logging.getLogger(__name__)


class DataHandler:
    """Classe responsável pela leitura (input) e escrita (output) de dados.

    Todos os dataframes utilizados pela aplicação são lidos com schema
    explícito (sem ``inferSchema``), evitando os riscos de desempenho,
    precisão e imprevisibilidade da inferência automática de schema.
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    @staticmethod
    def schema_pedidos() -> StructType:
        """Schema explícito do dataset de pedidos (CSV)."""
        return StructType(
            [
                StructField("id_pedido", StringType(), True),
                StructField("produto", StringType(), True),
                StructField("valor_unitario", FloatType(), True),
                StructField("quantidade", LongType(), True),
                StructField("data_criacao", TimestampType(), True),
                StructField("uf", StringType(), True),
                StructField("id_cliente", LongType(), True),
            ]
        )

    @staticmethod
    def schema_pagamentos() -> StructType:
        """Schema explícito do dataset de pagamentos (JSON), incluindo o
        objeto aninhado ``avaliacao_fraude``."""
        schema_avaliacao_fraude = StructType(
            [
                StructField("fraude", BooleanType(), True),
                StructField("score", FloatType(), True),
            ]
        )
        return StructType(
            [
                StructField("id_pedido", StringType(), True),
                StructField("forma_pagamento", StringType(), True),
                StructField("valor_pagamento", FloatType(), True),
                StructField("status", BooleanType(), True),
                StructField("data_processamento", TimestampType(), True),
                StructField("avaliacao_fraude", schema_avaliacao_fraude, True),
            ]
        )

    def load_pedidos(self, path: str, compression: str, header: bool, sep: str) -> DataFrame:
        """Carrega o dataframe de pedidos a partir dos arquivos CSV de origem."""
        logger.info(f"Lendo o dataframe de pedidos em '{path}'.")
        try:
            pedidos_df = (
                self._spark.read.option("compression", compression)
                .option("header", header)
                .option("sep", sep)
                .option("mode", "FAILFAST")
                .schema(self.schema_pedidos())
                .csv(path)
            )
            if pedidos_df.isEmpty():
                logger.warning(f"O dataframe de pedidos lido em '{path}' está vazio.")
            return pedidos_df
        except AnalysisException as erro:
            logger.error(f"Erro ao ler o dataframe de pedidos em '{path}': {erro}")
            raise

    def load_pagamentos(self, path: str, compression: str) -> DataFrame:
        """Carrega o dataframe de pagamentos a partir dos arquivos JSON de origem."""
        logger.info(f"Lendo o dataframe de pagamentos em '{path}'.")
        try:
            pagamentos_df = (
                self._spark.read.option("compression", compression)
                .option("mode", "FAILFAST")
                .schema(self.schema_pagamentos())
                .json(path)
            )
            if pagamentos_df.isEmpty():
                logger.warning(f"O dataframe de pagamentos lido em '{path}' está vazio.")
            return pagamentos_df
        except AnalysisException as erro:
            logger.error(f"Erro ao ler o dataframe de pagamentos em '{path}': {erro}")
            raise

    def write_parquet(self, df: DataFrame, path: str) -> None:
        """Grava o dataframe informado em formato parquet, sobrescrevendo o destino."""
        logger.info(f"Escrevendo o relatório em formato parquet em '{path}'.")
        df.write.mode("overwrite").parquet(path)
        logger.info(f"Relatório gravado com sucesso em '{path}'.")
