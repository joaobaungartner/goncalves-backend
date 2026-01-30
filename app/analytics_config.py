# app/analytics_config.py

COLLECTIONS = {
    "fatos": "fatos_pedidos",
    "polpa": "polpa_metricas",
    "manteiga": "manteiga_metricas",
}

# Campos permitidos por coleção (whitelist = segurança e previsibilidade)
FIELDS = {
    "fatos": [
        "id_pedido",
        "data_pedido",
        "tipo_produto",
        "mes_do_ano",
        "mes_do_ano_num",
        "canal",
        "regiao_destino",
        "cliente_segmento",
        "quantidade_kg",
        "preco_unitario_brl_kg",
        "nps_0a10",
    ],
    "polpa": [
        "id_pedido",
        "logistica_brl",
        "desconto_brl",
        "lote_id",
        "indice_qualidade_1a10",
        "perda_processamento_pct",
    ],
    "manteiga": [
        "id_pedido",
        "teor_umidade_pct",
        "indice_acidez_mgKOH_g",
        "ponto_fusao_c",
        "indice_oxidacao_1a10",
        "certificacao_exigida",
    ],
}

NUMERIC_FIELDS = {
    "fatos": ["quantidade_kg", "preco_unitario_brl_kg", "nps_0a10", "mes_do_ano_num"],
    "polpa": ["logistica_brl", "desconto_brl", "indice_qualidade_1a10", "perda_processamento_pct"],
    "manteiga": ["teor_umidade_pct", "indice_acidez_mgKOH_g", "ponto_fusao_c", "indice_oxidacao_1a10"],
}

CATEGORICAL_FIELDS = {
    "fatos": ["tipo_produto", "mes_do_ano", "canal", "regiao_destino", "cliente_segmento"],
    "polpa": ["lote_id"],
    "manteiga": ["certificacao_exigida"],
}
