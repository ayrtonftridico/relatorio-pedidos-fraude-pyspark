"""Testes unitários da classe de lógica de negócio (LogicaNegocioRelatorio).

Os DataFrames de entrada são construídos a partir de pequenos arquivos
CSV/JSON temporários (lidos com o próprio :class:`DataHandler`, com schema
explícito) em vez de ``spark.createDataFrame`` com listas Python. Esta
escolha evita um bug conhecido do PySpark em Windows + Python 3.12+
(SPARK-53759, EOFException no worker "simple-worker"), mantendo a suíte de
testes portável entre sistemas operacionais e versões de Python.
"""

import gzip
import json

import pytest

from io_utils.data_handler import DataHandler


@pytest.fixture
def logica():
    from processing.business_logic import LogicaNegocioRelatorio

    return LogicaNegocioRelatorio()


def _gravar_pedidos_csv(tmp_path, linhas_pedidos, nome_arquivo="pedidos.csv.gz"):
    """Grava um arquivo de pedidos no mesmo leiaute do dataset real (CSV;
    separador ';'; comprimido em gzip) e devolve o caminho do arquivo."""
    cabecalho = "id_pedido;produto;valor_unitario;quantidade;data_criacao;uf;id_cliente"
    linhas = [cabecalho] + [
        ";".join(str(valor) for valor in linha) for linha in linhas_pedidos
    ]
    caminho = tmp_path / nome_arquivo
    with gzip.open(caminho, "wt", encoding="utf-8") as arquivo:
        arquivo.write("\n".join(linhas))
    return str(caminho)


def _gravar_pagamentos_json(tmp_path, registros_pagamentos, nome_arquivo="pagamentos.json.gz"):
    """Grava um arquivo de pagamentos no mesmo leiaute do dataset real (JSON
    Lines comprimido em gzip) e devolve o caminho do arquivo."""
    linhas = [json.dumps(registro) for registro in registros_pagamentos]
    caminho = tmp_path / nome_arquivo
    with gzip.open(caminho, "wt", encoding="utf-8") as arquivo:
        arquivo.write("\n".join(linhas))
    return str(caminho)


def _carregar_pedidos(spark, tmp_path, linhas_pedidos):
    caminho = _gravar_pedidos_csv(tmp_path, linhas_pedidos)
    return DataHandler(spark).load_pedidos(
        path=caminho, compression="gzip", header=True, sep=";"
    )


def _carregar_pagamentos(spark, tmp_path, registros_pagamentos):
    caminho = _gravar_pagamentos_json(tmp_path, registros_pagamentos)
    return DataHandler(spark).load_pagamentos(path=caminho, compression="gzip")


class TestAdicionarValorTotal:
    def test_calcula_valor_total_como_unitario_vezes_quantidade(self, spark, tmp_path, logica):
        pedidos_df = _carregar_pedidos(
            spark,
            tmp_path,
            [("p1", "NOTEBOOK", 1500.0, 2, "2025-01-10T10:00:00", "SP", 100)],
        )

        resultado_df = logica.adicionar_valor_total(pedidos_df)
        linha = resultado_df.collect()[0]

        assert linha["valor_total"] == pytest.approx(3000.0)

    def test_quantidade_zero_resulta_em_valor_total_zero(self, spark, tmp_path, logica):
        """Caso de borda: quantidade zero não deve gerar erro nem valor negativo."""
        pedidos_df = _carregar_pedidos(
            spark,
            tmp_path,
            [("p1", "MOUSE", 100.0, 0, "2025-01-10T10:00:00", "RJ", 100)],
        )

        resultado_df = logica.adicionar_valor_total(pedidos_df)
        linha = resultado_df.collect()[0]

        assert linha["valor_total"] == 0.0


class TestFiltrarPedidosPorAno:
    def test_mantem_apenas_pedidos_do_ano_informado(self, spark, tmp_path, logica):
        pedidos_df = _carregar_pedidos(
            spark,
            tmp_path,
            [
                ("p2024", "TV", 1000.0, 1, "2024-12-31T23:59:59", "MG", 1),
                ("p2025", "TV", 1000.0, 1, "2025-01-01T00:00:00", "MG", 2),
                ("p2025b", "TV", 1000.0, 1, "2025-12-31T23:59:59", "MG", 3),
                ("p2026", "TV", 1000.0, 1, "2026-01-01T00:00:00", "MG", 4),
            ],
        )

        resultado_df = logica.filtrar_pedidos_por_ano(pedidos_df, ano=2025)
        ids_resultado = {linha["id_pedido"] for linha in resultado_df.collect()}

        assert ids_resultado == {"p2025", "p2025b"}


class TestFiltrarPagamentosRecusadosLegitimos:
    @pytest.mark.parametrize(
        "status, fraude, deve_aparecer",
        [
            (True, False, False),  # aprovado e legítimo -> não interessa ao relatório
            (False, True, False),  # recusado e fraudulento -> não interessa
            (False, False, True),  # recusado e legítimo -> é exatamente o que queremos
            (True, True, False),  # aprovado e fraudulento -> não interessa
        ],
    )
    def test_apenas_pagamentos_recusados_e_legitimos_sao_mantidos(
        self, spark, tmp_path, logica, status, fraude, deve_aparecer
    ):
        pagamentos_df = _carregar_pagamentos(
            spark,
            tmp_path,
            [
                {
                    "id_pedido": "p1",
                    "forma_pagamento": "PIX",
                    "valor_pagamento": 1500.0,
                    "status": status,
                    "data_processamento": "2025-01-10T10:00:00",
                    "avaliacao_fraude": {"fraude": fraude, "score": 0.5},
                }
            ],
        )

        resultado_df = logica.filtrar_pagamentos_recusados_legitimos(pagamentos_df)

        assert (resultado_df.count() == 1) == deve_aparecer


class TestMontarRelatorio:
    def test_relatorio_contem_apenas_as_colunas_exigidas(self, spark, tmp_path, logica):
        pedidos_df = logica.adicionar_valor_total(
            _carregar_pedidos(
                spark,
                tmp_path,
                [("p1", "NOTEBOOK", 1500.0, 1, "2025-01-10T10:00:00", "SP", 1)],
            )
        )

        pagamentos_df = _carregar_pagamentos(
            spark,
            tmp_path,
            [
                {
                    "id_pedido": "p1",
                    "forma_pagamento": "PIX",
                    "valor_pagamento": 1500.0,
                    "status": False,
                    "data_processamento": "2025-01-10T11:00:00",
                    "avaliacao_fraude": {"fraude": False, "score": 0.2},
                }
            ],
        )

        relatorio_df = logica.montar_relatorio(pedidos_df, pagamentos_df)

        assert relatorio_df.columns == [
            "id_pedido",
            "uf",
            "forma_pagamento",
            "valor_total",
            "data_pedido",
        ]

        linha = relatorio_df.collect()[0]
        assert linha["id_pedido"] == "p1"
        assert linha["uf"] == "SP"
        assert linha["forma_pagamento"] == "PIX"
        assert linha["valor_total"] == pytest.approx(1500.0)


class TestOrdenarRelatorio:
    def test_ordena_por_uf_forma_pagamento_e_data_pedido(self, spark, tmp_path, logica):
        pedidos_df = logica.adicionar_valor_total(
            _carregar_pedidos(
                spark,
                tmp_path,
                [
                    ("p1", "TV", 1000.0, 1, "2025-03-01T00:00:00", "SP", 1),
                    ("p2", "TV", 1000.0, 1, "2025-01-01T00:00:00", "SP", 2),
                    ("p3", "TV", 1000.0, 1, "2025-02-01T00:00:00", "RJ", 3),
                ],
            )
        )

        pagamentos_df = _carregar_pagamentos(
            spark,
            tmp_path,
            [
                {
                    "id_pedido": "p1",
                    "forma_pagamento": "PIX",
                    "valor_pagamento": 1000.0,
                    "status": False,
                    "data_processamento": "2025-03-01T01:00:00",
                    "avaliacao_fraude": {"fraude": False, "score": 0.1},
                },
                {
                    "id_pedido": "p2",
                    "forma_pagamento": "BOLETO",
                    "valor_pagamento": 1000.0,
                    "status": False,
                    "data_processamento": "2025-01-01T01:00:00",
                    "avaliacao_fraude": {"fraude": False, "score": 0.1},
                },
                {
                    "id_pedido": "p3",
                    "forma_pagamento": "PIX",
                    "valor_pagamento": 1000.0,
                    "status": False,
                    "data_processamento": "2025-02-01T01:00:00",
                    "avaliacao_fraude": {"fraude": False, "score": 0.1},
                },
            ],
        )

        relatorio_df = logica.montar_relatorio(pedidos_df, pagamentos_df)
        relatorio_ordenado_df = logica.ordenar_relatorio(relatorio_df)

        ids_na_ordem = [linha["id_pedido"] for linha in relatorio_ordenado_df.collect()]

        # RJ vem antes de SP; dentro de SP, BOLETO (p2) vem antes de PIX (p1).
        assert ids_na_ordem == ["p3", "p2", "p1"]


class TestGerarRelatorio:
    def test_cenario_completo_conforme_amostras_do_dataset_de_pagamentos(
        self, spark, tmp_path, logica
    ):
        """Reproduz os três cenários de amostra descritos no README do dataset
        de pagamentos: apenas o pedido recusado e legítimo deve aparecer."""
        pedidos_df = _carregar_pedidos(
            spark,
            tmp_path,
            [
                (
                    "9fc9c6b8-6dda-44c9-a780-dbfa6e394a84",
                    "CARTAO",
                    1800.0,
                    1,
                    "2025-05-10T10:00:00",
                    "SP",
                    1,
                ),
                (
                    "3b831b7e-7aa3-41aa-b5b2-5bdcdcfef19e",
                    "CELULAR",
                    1500.0,
                    1,
                    "2025-06-01T10:00:00",
                    "RJ",
                    2,
                ),
                (
                    "81024c98-bc43-4844-b46d-ea26d009c1b7",
                    "GELADEIRA",
                    1500.0,
                    1,
                    "2025-07-20T10:00:00",
                    "MG",
                    3,
                ),
                (
                    "pedido-fora-do-ano",
                    "TV",
                    900.0,
                    1,
                    "2024-12-31T10:00:00",
                    "BA",
                    4,
                ),
            ],
        )

        pagamentos_df = _carregar_pagamentos(
            spark,
            tmp_path,
            [
                # 1. Aprovado e legítimo -> não deve aparecer.
                {
                    "id_pedido": "9fc9c6b8-6dda-44c9-a780-dbfa6e394a84",
                    "forma_pagamento": "CARTAO_CREDITO",
                    "valor_pagamento": 1800.0,
                    "status": True,
                    "data_processamento": "2025-05-10T11:00:00",
                    "avaliacao_fraude": {"fraude": False, "score": 0.10},
                },
                # 2. Recusado e fraudulento -> não deve aparecer.
                {
                    "id_pedido": "3b831b7e-7aa3-41aa-b5b2-5bdcdcfef19e",
                    "forma_pagamento": "PIX",
                    "valor_pagamento": 1500.0,
                    "status": False,
                    "data_processamento": "2025-06-01T11:00:00",
                    "avaliacao_fraude": {"fraude": True, "score": 0.99},
                },
                # 3. Recusado e legítimo -> é o único que deve aparecer no relatório.
                {
                    "id_pedido": "81024c98-bc43-4844-b46d-ea26d009c1b7",
                    "forma_pagamento": "BOLETO",
                    "valor_pagamento": 1500.0,
                    "status": False,
                    "data_processamento": "2025-07-20T11:00:00",
                    "avaliacao_fraude": {"fraude": False, "score": 0.20},
                },
                # 4. Recusado e legítimo, mas o pedido é de 2024 -> não deve aparecer.
                {
                    "id_pedido": "pedido-fora-do-ano",
                    "forma_pagamento": "PIX",
                    "valor_pagamento": 900.0,
                    "status": False,
                    "data_processamento": "2024-12-31T11:00:00",
                    "avaliacao_fraude": {"fraude": False, "score": 0.05},
                },
            ],
        )

        relatorio_df = logica.gerar_relatorio(pedidos_df, pagamentos_df, ano=2025)
        resultado = relatorio_df.collect()

        assert len(resultado) == 1
        linha = resultado[0]
        assert linha["id_pedido"] == "81024c98-bc43-4844-b46d-ea26d009c1b7"
        assert linha["uf"] == "MG"
        assert linha["forma_pagamento"] == "BOLETO"
        assert linha["valor_total"] == pytest.approx(1500.0)

    def test_propaga_e_loga_erro_quando_dataframe_de_entrada_e_invalido(
        self, spark, tmp_path, logica, caplog
    ):
        """Garante o tratamento de erros: uma falha na transformação deve ser
        capturada, registrada via logging e relançada (não engolida)."""
        pedidos_df_invalido = spark.read.json(
            str(_gravar_pagamentos_json(tmp_path, [{"id_pedido": "p1"}], "pedidos_invalidos.json.gz"))
        )  # faltam colunas obrigatórias, como valor_unitario e quantidade.

        pagamentos_df = _carregar_pagamentos(
            spark,
            tmp_path,
            [
                {
                    "id_pedido": "p1",
                    "forma_pagamento": "PIX",
                    "valor_pagamento": 100.0,
                    "status": False,
                    "data_processamento": "2025-01-01T00:00:00",
                    "avaliacao_fraude": {"fraude": False, "score": 0.1},
                }
            ],
        )

        with pytest.raises(Exception):
            logica.gerar_relatorio(pedidos_df_invalido, pagamentos_df, ano=2025)

        assert "Erro ao gerar o relatório de pedidos" in caplog.text
