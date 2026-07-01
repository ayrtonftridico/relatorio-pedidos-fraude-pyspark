"""Pacote de orquestração do pipeline."""

import logging

from config.settings import AppConfig
from io_utils.data_handler import DataHandler
from processing.business_logic import LogicaNegocioRelatorio

logger = logging.getLogger(__name__)


class PipelineRelatorioPedidos:
    """Classe que orquestra as etapas do pipeline: leitura, transformação e escrita.

    Todas as suas dependências (configuração, leitura/escrita de dados e
    lógica de negócio) são recebidas via construtor (injeção de
    dependências), e não criadas internamente. Isso mantém a classe
    facilmente testável com dependências falsas (mocks/stubs).
    """

    def __init__(
        self,
        config: AppConfig,
        data_handler: DataHandler,
        logica_negocio: LogicaNegocioRelatorio,
    ):
        self._config = config
        self._data_handler = data_handler
        self._logica_negocio = logica_negocio

    def executar(self) -> None:
        """Executa o pipeline completo de geração do relatório de pedidos."""
        logger.info("Pipeline iniciado.")

        pedidos_df = self._data_handler.load_pedidos(
            path=self._config.path_pedidos,
            **self._config.pedidos_csv_options,
        )

        pagamentos_df = self._data_handler.load_pagamentos(
            path=self._config.path_pagamentos,
            **self._config.pagamentos_json_options,
        )

        relatorio_df = self._logica_negocio.gerar_relatorio(
            pedidos_df=pedidos_df,
            pagamentos_df=pagamentos_df,
            ano=self._config.ano_referencia,
        )

        logger.info("Amostra do relatório gerado (até 20 linhas):")
        relatorio_df.show(20, truncate=False)

        self._data_handler.write_parquet(df=relatorio_df, path=self._config.path_output)

        logger.info("Pipeline finalizado com sucesso.")
