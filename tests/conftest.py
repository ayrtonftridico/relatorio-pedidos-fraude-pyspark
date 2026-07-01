"""Fixtures compartilhadas por toda a suíte de testes."""

import os
import sys

import pytest
from pyspark.sql import SparkSession

# Mesmo motivo do spark_session.py: fixa o driver e os workers no Python
# que está rodando os testes.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    """SparkSession compartilhada por toda a suíte de testes.

    ``scope="session"`` garante que a sessão seja criada uma única vez e
    reutilizada por todos os testes, evitando o custo de inicializar o Spark
    repetidamente.
    """
    session = (
        SparkSession.builder.appName("test-relatorio-pedidos-fraude-pyspark")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()
