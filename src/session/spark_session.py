"""Pacote de gerenciamento da sessão Spark."""

import logging
import os
import sys

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Fixa o driver e os workers do PySpark no mesmo interpretador Python da
# aplicação, para não depender de qual Python está primeiro no PATH.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


class SparkSessionManager:
    """Classe responsável por criar, fornecer e finalizar a SparkSession.

    Encapsular esta responsabilidade evita que a configuração da sessão
    Spark fique espalhada pelo código e facilita a substituição por uma
    sessão de testes (por exemplo, ``master="local[2]"``) sem alterar as
    demais classes da aplicação.
    """

    def __init__(self, app_name: str, master: str = "local[*]"):
        self._app_name = app_name
        self._master = master
        self._spark_session = None

    def get_spark_session(self) -> SparkSession:
        """Cria (se necessário) e retorna a SparkSession da aplicação."""
        if self._spark_session is None:
            logger.info(
                f"Criando a SparkSession '{self._app_name}' (master='{self._master}')."
            )
            self._spark_session = (
                SparkSession.builder.appName(self._app_name)
                .master(self._master)
                .getOrCreate()
            )
        return self._spark_session

    def stop(self) -> None:
        """Finaliza a SparkSession, caso esteja ativa."""
        if self._spark_session is not None:
            self._spark_session.stop()
            self._spark_session = None
