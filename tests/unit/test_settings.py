"""Testes unitários da classe de configuração centralizada (AppConfig)."""

from config.settings import AppConfig


def test_app_config_carrega_valores_do_settings_yaml_padrao():
    config = AppConfig()

    assert config.spark_app_name == "RelatorioPedidosFraudePySpark"
    assert config.spark_master == "local[*]"
    assert config.ano_referencia == 2025
    assert "pedidos" in config.path_pedidos
    assert "pagamentos" in config.path_pagamentos
    assert config.pedidos_csv_options == {
        "compression": "gzip",
        "header": True,
        "sep": ";",
    }
    assert config.pagamentos_json_options == {"compression": "gzip"}


def test_app_config_aceita_arquivo_de_configuracao_customizado(tmp_path):
    config_customizado = tmp_path / "settings_customizado.yaml"
    config_customizado.write_text(
        """
spark:
  app_name: "AppDeTeste"
  master: "local[1]"
paths:
  pedidos: "/tmp/pedidos"
  pagamentos: "/tmp/pagamentos"
  output: "/tmp/output"
file_options:
  pedidos_csv:
    compression: "gzip"
    header: true
    sep: ";"
  pagamentos_json:
    compression: "gzip"
regras_negocio:
  ano_referencia: 2030
""",
        encoding="utf-8",
    )

    config = AppConfig(config_path=str(config_customizado))

    assert config.spark_app_name == "AppDeTeste"
    assert config.ano_referencia == 2030
    assert config.path_output == "/tmp/output"
