"""Pacote de configurações centralizadas da aplicação.

Concentra em uma única classe todos os parâmetros utilizados pelo restante
do projeto (nome da aplicação Spark, caminhos de entrada/saída, opções de
leitura dos arquivos e regras de negócio configuráveis), evitando o uso de
"strings mágicas" espalhadas pelo código.
"""

import os

import yaml

CONFIG_FILENAME = "settings.yaml"


class AppConfig:
    """Classe de configuração centralizada da aplicação.

    Carrega o arquivo ``settings.yaml`` e expõe seus valores através de
    propriedades fortemente tipadas, para que o restante do projeto nunca
    precise acessar o dicionário de configuração diretamente.
    """

    def __init__(self, config_path: str = None):
        self._config_path = config_path or self._default_config_path()
        self._config = self._carregar_arquivo_yaml(self._config_path)

    @staticmethod
    def _default_config_path() -> str:
        """Resolve o caminho padrão de ``settings.yaml`` relativo a este módulo.

        Resolver o caminho a partir de ``__file__`` (em vez de um caminho fixo)
        garante que a configuração seja encontrada independentemente do
        diretório de onde a aplicação é executada ou do sistema operacional.
        """
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), CONFIG_FILENAME)

    @staticmethod
    def _carregar_arquivo_yaml(path: str) -> dict:
        with open(path, "r", encoding="utf-8") as arquivo_config:
            return yaml.safe_load(arquivo_config)

    @property
    def spark_app_name(self) -> str:
        return self._config["spark"]["app_name"]

    @property
    def spark_master(self) -> str:
        return self._config["spark"].get("master", "local[*]")

    @property
    def path_pedidos(self) -> str:
        return self._config["paths"]["pedidos"]

    @property
    def path_pagamentos(self) -> str:
        return self._config["paths"]["pagamentos"]

    @property
    def path_output(self) -> str:
        return self._config["paths"]["output"]

    @property
    def pedidos_csv_options(self) -> dict:
        return dict(self._config["file_options"]["pedidos_csv"])

    @property
    def pagamentos_json_options(self) -> dict:
        return dict(self._config["file_options"]["pagamentos_json"])

    @property
    def ano_referencia(self) -> int:
        return self._config["regras_negocio"]["ano_referencia"]
