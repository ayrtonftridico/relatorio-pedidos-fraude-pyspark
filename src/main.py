"""Ponto de entrada (Aggregation Root) da aplicação.

É aqui que:
    1. O logging da aplicação é configurado (a configuração vive na classe
       de lógica de negócio, mas é disparada a partir daqui);
    2. Todas as dependências concretas são instanciadas (configuração,
       sessão Spark, leitura/escrita de dados, lógica de negócio e
       orquestração);
    3. Essas dependências são injetadas na classe que orquestra o pipeline;
    4. A sessão Spark é encerrada com segurança, mesmo se algo der errado.
"""

import logging

from config.settings import AppConfig
from io_utils.data_handler import DataHandler
from pipeline.pipeline import PipelineRelatorioPedidos
from processing.business_logic import LogicaNegocioRelatorio
from session.spark_session import SparkSessionManager


def main() -> None:
    """Monta e executa o pipeline de geração do relatório de pedidos.

    Esta função é a raiz de composição (Aggregation Root) da aplicação: o
    único lugar onde as dependências concretas são criadas e injetadas nas
    classes que precisam delas.
    """
    logger = logging.getLogger(__name__)

    config = AppConfig()

    spark_session_manager = SparkSessionManager(
        app_name=config.spark_app_name,
        master=config.spark_master,
    )

    spark = None
    try:
        spark = spark_session_manager.get_spark_session()

        data_handler = DataHandler(spark=spark)
        logica_negocio = LogicaNegocioRelatorio()

        pipeline = PipelineRelatorioPedidos(
            config=config,
            data_handler=data_handler,
            logica_negocio=logica_negocio,
        )

        pipeline.executar()

    except Exception as erro:
        logger.error(f"Falha crítica na execução do pipeline: {erro}")
        raise
    finally:
        if spark is not None:
            spark_session_manager.stop()
            logger.info("Sessão Spark finalizada.")


if __name__ == "__main__":
    LogicaNegocioRelatorio.configurar_logging()
    main()
