"""Pacote de lógica de negócio.

Aqui ficam as regras que resolvem o que a alta gestão pediu: montar o
relatório de pedidos cujo pagamento foi recusado (``status=false``) e que a
avaliação de fraude classificou como legítimo (``fraude=false``), restrito
ao ano de referência configurado.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

NOME_ARQUIVO_LOG = "relatorio-pedidos-fraude-pyspark.log"


class LogicaNegocioRelatorio:
    """Classe com as regras de negócio do relatório de pedidos.

    Cada método público é uma regra isolada e testável. O método
    :meth:`gerar_relatorio` orquestra as demais regras para montar o
    relatório final.
    """

    COLUNAS_RELATORIO = [
        "id_pedido",
        "uf",
        "forma_pagamento",
        "valor_total",
        "data_pedido",
    ]

    @staticmethod
    def configurar_logging() -> None:
        """Configura o logging da aplicação.

        A configuração fica na própria classe de lógica de negócio, em vez
        de em ``main.py``, que apenas chama este método antes de montar o
        restante do pipeline.
        """
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(NOME_ARQUIVO_LOG, encoding="utf-8"),
                logging.StreamHandler(),
            ],
        )

    def adicionar_valor_total(self, pedidos_df: DataFrame) -> DataFrame:
        """Adiciona a coluna ``valor_total`` (valor_unitario * quantidade)."""
        logger.info("Calculando o valor total de cada pedido (valor_unitario * quantidade).")
        return pedidos_df.withColumn(
            "valor_total", F.col("valor_unitario") * F.col("quantidade")
        )

    def filtrar_pedidos_por_ano(self, pedidos_df: DataFrame, ano: int) -> DataFrame:
        """Filtra os pedidos cuja data de criação pertence ao ano informado."""
        logger.info(f"Filtrando pedidos do ano de referência {ano}.")
        return pedidos_df.filter(F.year(F.col("data_criacao")) == ano)

    def filtrar_pagamentos_recusados_legitimos(self, pagamentos_df: DataFrame) -> DataFrame:
        """Filtra pagamentos recusados (``status=false``) e avaliados como
        legítimos pela análise de fraude (``avaliacao_fraude.fraude=false``)."""
        logger.info(
            "Filtrando pagamentos recusados (status=false) e classificados "
            "como legítimos na avaliação de fraude (fraude=false)."
        )
        return pagamentos_df.filter(
            (F.col("status") == F.lit(False))
            & (F.col("avaliacao_fraude.fraude") == F.lit(False))
        )

    def montar_relatorio(self, pedidos_df: DataFrame, pagamentos_df: DataFrame) -> DataFrame:
        """Junta pedidos e pagamentos pelo identificador do pedido e seleciona
        somente os atributos exigidos pelo relatório."""
        logger.info("Unindo pedidos e pagamentos pelo identificador do pedido (id_pedido).")
        return pedidos_df.join(
            pagamentos_df,
            pedidos_df.id_pedido == pagamentos_df.id_pedido,
            "inner",
        ).select(
            pedidos_df.id_pedido.alias("id_pedido"),
            pedidos_df.uf.alias("uf"),
            pagamentos_df.forma_pagamento.alias("forma_pagamento"),
            pedidos_df.valor_total.alias("valor_total"),
            pedidos_df.data_criacao.alias("data_pedido"),
        )

    def ordenar_relatorio(self, relatorio_df: DataFrame) -> DataFrame:
        """Ordena o relatório por UF, forma de pagamento e data de criação do pedido."""
        logger.info("Ordenando o relatório por uf, forma_pagamento e data_pedido.")
        return relatorio_df.orderBy("uf", "forma_pagamento", "data_pedido")

    def gerar_relatorio(
        self, pedidos_df: DataFrame, pagamentos_df: DataFrame, ano: int
    ) -> DataFrame:
        """Orquestra as regras de negócio para gerar o relatório final.

        :param pedidos_df: Dataframe de pedidos (lido pelo DataHandler).
        :param pagamentos_df: Dataframe de pagamentos (lido pelo DataHandler).
        :param ano: Ano de referência dos pedidos que devem compor o relatório.
        :return: Dataframe final do relatório, já filtrado e ordenado.
        :raises Exception: Relança qualquer exceção ocorrida durante o
            processamento, após registrar o erro no log.
        """
        try:
            logger.info("Iniciando a geração do relatório de pedidos recusados e legítimos.")

            pedidos_com_valor_total_df = self.adicionar_valor_total(pedidos_df)
            pedidos_do_ano_df = self.filtrar_pedidos_por_ano(pedidos_com_valor_total_df, ano)
            pagamentos_filtrados_df = self.filtrar_pagamentos_recusados_legitimos(pagamentos_df)

            relatorio_df = self.montar_relatorio(pedidos_do_ano_df, pagamentos_filtrados_df)
            relatorio_ordenado_df = self.ordenar_relatorio(relatorio_df)

            logger.info("Relatório de pedidos gerado com sucesso.")
            return relatorio_ordenado_df

        except Exception as erro:
            logger.error(f"Erro ao gerar o relatório de pedidos: {erro}")
            raise
