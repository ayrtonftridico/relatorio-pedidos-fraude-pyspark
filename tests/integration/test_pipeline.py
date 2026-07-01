"""Teste de integração da classe de orquestração (PipelineRelatorioPedidos).

Verifica se Pipeline, DataHandler e LogicaNegocioRelatorio cooperam
corretamente. O DataHandler real é utilizado para a leitura, mas a partir
de um diretório local com uma pequena amostra de dados, simulando o que
aconteceria na execução real, sem depender dos repositórios externos.
"""

import gzip
import json
import shutil

import pytest

from config.settings import AppConfig
from io_utils.data_handler import DataHandler
from pipeline.pipeline import PipelineRelatorioPedidos
from processing.business_logic import LogicaNegocioRelatorio


class ConfigDeTeste(AppConfig):
    """Reaproveita a classe AppConfig, mas substitui os caminhos para
    apontar para os arquivos de amostra criados no diretório temporário."""

    def __init__(self, dir_pedidos, dir_pagamentos, dir_output):
        # Não chama o __init__ da classe-mãe: aqui não há settings.yaml,
        # os valores de configuração são fornecidos diretamente.
        self._config = {
            "spark": {"app_name": "teste-pipeline", "master": "local[2]"},
            "paths": {
                "pedidos": dir_pedidos,
                "pagamentos": dir_pagamentos,
                "output": dir_output,
            },
            "file_options": {
                "pedidos_csv": {"compression": "gzip", "header": True, "sep": ";"},
                "pagamentos_json": {"compression": "gzip"},
            },
            "regras_negocio": {"ano_referencia": 2025},
        }


@pytest.fixture
def amostra_de_dados(tmp_path):
    dir_pedidos = tmp_path / "pedidos"
    dir_pagamentos = tmp_path / "pagamentos"
    dir_output = tmp_path / "output" / "relatorio"
    dir_pedidos.mkdir()
    dir_pagamentos.mkdir()

    conteudo_csv = (
        "id_pedido;produto;valor_unitario;quantidade;data_criacao;uf;id_cliente\n"
        "p1;NOTEBOOK;1500.0;1;2025-01-10T10:00:00;SP;1\n"
        "p2;CELULAR;1000.0;2;2025-02-15T08:30:00;RJ;2\n"
    )
    with gzip.open(dir_pedidos / "pedidos-2025-01.csv.gz", "wt", encoding="utf-8") as arquivo:
        arquivo.write(conteudo_csv)

    registros_pagamentos = [
        {
            "id_pedido": "p1",
            "forma_pagamento": "PIX",
            "valor_pagamento": 1500.0,
            "status": False,
            "data_processamento": "2025-01-10T11:00:00",
            "avaliacao_fraude": {"fraude": False, "score": 0.1},
        },
        {
            "id_pedido": "p2",
            "forma_pagamento": "BOLETO",
            "valor_pagamento": 2000.0,
            "status": True,
            "data_processamento": "2025-02-15T09:00:00",
            "avaliacao_fraude": {"fraude": False, "score": 0.2},
        },
    ]
    linhas_json = "\n".join(json.dumps(registro) for registro in registros_pagamentos)
    with gzip.open(
        dir_pagamentos / "pagamentos-2025-01.json.gz", "wt", encoding="utf-8"
    ) as arquivo:
        arquivo.write(linhas_json)

    yield {
        "pedidos": str(dir_pedidos) + "/",
        "pagamentos": str(dir_pagamentos) + "/",
        "output": str(dir_output),
    }

    shutil.rmtree(tmp_path, ignore_errors=True)


@pytest.mark.integration
def test_pipeline_executa_ponta_a_ponta_e_grava_parquet(spark, amostra_de_dados):
    config = ConfigDeTeste(
        dir_pedidos=amostra_de_dados["pedidos"],
        dir_pagamentos=amostra_de_dados["pagamentos"],
        dir_output=amostra_de_dados["output"],
    )

    pipeline = PipelineRelatorioPedidos(
        config=config,
        data_handler=DataHandler(spark=spark),
        logica_negocio=LogicaNegocioRelatorio(),
    )

    pipeline.executar()

    relatorio_df = spark.read.parquet(amostra_de_dados["output"])
    resultado = relatorio_df.collect()

    assert len(resultado) == 1
    assert resultado[0]["id_pedido"] == "p1"
    assert resultado[0]["forma_pagamento"] == "PIX"
