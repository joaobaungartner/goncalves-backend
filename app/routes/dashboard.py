"""
Endpoints do dashboard por segmento/modalidade da sidebar.
Cada seção expõe os dados necessários para os gráficos e KPIs descritos.
"""
from fastapi import APIRouter, Query
from typing import Optional, List
from datetime import datetime, timedelta
from app.db import fatos, polpa, manteiga

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


def _match_fatos(
    tipo_produto: Optional[str] = None,
    mes_do_ano_num: Optional[int] = None,
    canal: Optional[str] = None,
    regiao_destino: Optional[str] = None,
    cliente_segmento: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    q = {}
    if tipo_produto:
        q["tipo_produto"] = tipo_produto
    if mes_do_ano_num is not None:
        q["mes_do_ano_num"] = mes_do_ano_num
    if canal:
        q["canal"] = canal
    if regiao_destino:
        q["regiao_destino"] = regiao_destino
    if cliente_segmento:
        q["cliente_segmento"] = cliente_segmento
    if date_from or date_to:
        dt = {}
        if date_from:
            dt["$gte"] = datetime.fromisoformat(date_from.replace("Z", "+00:00"))
        if date_to:
            dt["$lte"] = datetime.fromisoformat(date_to.replace("Z", "+00:00"))
        q["data_pedido"] = dt
    return q


def _receita_expr():
    return {"$multiply": ["$quantidade_kg", "$preco_unitario_brl_kg"]}


# ---------- 1. Visão Geral ----------
def _parse_date_to_year_month(value) -> Optional[tuple]:
    """Extrai (ano, mês) de datetime ou string ISO. Retorna None se inválido."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return (value.year, value.month)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return (dt.year, dt.month)
        except (ValueError, TypeError):
            return None
    return None


@router.get("/visao-geral")
async def visao_geral(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    mes_atual: Optional[int] = None,
    ano_atual: Optional[int] = None,
):
    """
    KPIs: faturamento total, volume kg, nº pedidos, ticket médio, NPS médio.
    Comparação mês atual vs mês anterior.
    Se date_from/date_to não forem informados, mes_atual = último mês com dados no banco.
    """
    today = datetime.utcnow()
    ano = ano_atual
    mes = mes_atual

    if date_from and date_to:
        # Período explícito: usa como "atual" e calcula "anterior" com mesmo tamanho
        start_current = datetime.fromisoformat(date_from.replace("Z", "+00:00"))
        end_current = datetime.fromisoformat(date_to.replace("Z", "+00:00"))
        delta = end_current - start_current
        end_prev = start_current - timedelta(microseconds=1)
        start_prev = end_prev - delta
        ano = start_current.year
        mes = start_current.month
    else:
        # Sem período explícito: mes_atual = último mês com dados no banco
        if ano is None or mes is None:
            pipeline_ultimo = [
                {"$match": {"data_pedido": {"$ne": None}}},
                {"$group": {"_id": None, "max_date": {"$max": "$data_pedido"}}},
            ]
            res_ultimo = await fatos.aggregate(pipeline_ultimo).to_list(1)
            if res_ultimo and res_ultimo[0].get("max_date"):
                parsed = _parse_date_to_year_month(res_ultimo[0]["max_date"])
                if parsed:
                    ano, mes = parsed
            if ano is None:
                ano = today.year
            if mes is None:
                mes = today.month

        # Intervalos do mês atual (último com dados ou informado)
        start_current = datetime(ano, mes, 1)
        if mes == 12:
            end_current = datetime(ano + 1, 1, 1) - timedelta(microseconds=1)
        else:
            end_current = datetime(ano, mes + 1, 1) - timedelta(microseconds=1)

        # Mês anterior
        if mes == 1:
            start_prev = datetime(ano - 1, 12, 1)
            end_prev = datetime(ano, 1, 1) - timedelta(microseconds=1)
        else:
            start_prev = datetime(ano, mes - 1, 1)
            end_prev = start_current - timedelta(microseconds=1)

    match_current = {"data_pedido": {"$gte": start_current, "$lte": end_current}}
    match_prev = {"data_pedido": {"$gte": start_prev, "$lte": end_prev}}

    base_pipeline = [
        {"$addFields": {"receita_estimada": _receita_expr()}},
        {"$group": {
            "_id": None,
            "faturamento_total": {"$sum": "$receita_estimada"},
            "volume_kg": {"$sum": "$quantidade_kg"},
            "num_pedidos": {"$sum": 1},
            "nps_medio": {"$avg": "$nps_0a10"},
        }},
        {"$addFields": {
            "ticket_medio": {"$cond": [
                {"$eq": ["$num_pedidos", 0]},
                None,
                {"$divide": ["$faturamento_total", "$num_pedidos"]}
            ]}
        }},
        {"$project": {"_id": 0}}
    ]

    pipe_current = [{"$match": match_current}] + base_pipeline
    pipe_prev = [{"$match": match_prev}] + base_pipeline

    res_current = await fatos.aggregate(pipe_current).to_list(1)
    res_prev = await fatos.aggregate(pipe_prev).to_list(1)

    current = res_current[0] if res_current else {
        "faturamento_total": 0, "volume_kg": 0, "num_pedidos": 0,
        "ticket_medio": None, "nps_medio": None
    }
    prev = res_prev[0] if res_prev else {
        "faturamento_total": 0, "volume_kg": 0, "num_pedidos": 0,
        "ticket_medio": None, "nps_medio": None
    }

    def _var(current_val, prev_val):
        if prev_val is None or prev_val == 0:
            return None
        return round((float(current_val or 0) - float(prev_val)) / float(prev_val) * 100, 2)

    return {
        "mes_atual": {"year": ano, "month": mes},
        "kpis_atual": current,
        "kpis_mes_anterior": prev,
        "variacao_pct": {
            "faturamento": _var(current.get("faturamento_total"), prev.get("faturamento_total")),
            "volume_kg": _var(current.get("volume_kg"), prev.get("volume_kg")),
            "num_pedidos": _var(current.get("num_pedidos"), prev.get("num_pedidos")),
            "ticket_medio": _var(current.get("ticket_medio"), prev.get("ticket_medio")),
            "nps_medio": _var(current.get("nps_medio"), prev.get("nps_medio")),
        }
    }


@router.get("/visao-geral/serie-faturamento")
async def visao_geral_serie_faturamento(
    granularity: str = Query("month", pattern="^(day|month)$"),
    meses: int = Query(12, ge=1, le=24),
):
    """Série temporal de faturamento para o gráfico da Visão Geral."""
    end = datetime.utcnow()
    start = end - timedelta(days=meses * 31)
    match = {"data_pedido": {"$gte": start, "$lte": end}}
    date_group = {"year": {"$year": "$data_pedido"}, "month": {"$month": "$data_pedido"}}
    if granularity == "day":
        date_group["day"] = {"$dayOfMonth": "$data_pedido"}
    pipeline = [
        {"$match": match},
        {"$addFields": {"receita_estimada": _receita_expr()}},
        {"$group": {"_id": date_group, "faturamento": {"$sum": "$receita_estimada"}}},
        {"$sort": {"_id.year": 1, "_id.month": 1, **({"_id.day": 1} if granularity == "day" else {})}},
        {"$project": {"_id": 0, "year": "$_id.year", "month": "$_id.month", **({"day": "$_id.day"} if granularity == "day" else {}), "faturamento": 1}}
    ]
    items = await fatos.aggregate(pipeline).to_list(10000)
    return {"granularity": granularity, "items": items}


@router.get("/visao-geral/distribuicao-vendas-produto")
async def visao_geral_distribuicao_produto(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = Query(20, ge=5, le=100),
):
    """Distribuição de vendas (faturamento) por produto para donut/treemap."""
    match = _match_fatos(date_from=date_from, date_to=date_to)
    pipeline = [
        {"$match": match},
        {"$addFields": {"receita_estimada": _receita_expr()}},
        {"$group": {"_id": "$tipo_produto", "faturamento": {"$sum": "$receita_estimada"}, "volume_kg": {"$sum": "$quantidade_kg"}}},
        {"$sort": {"faturamento": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "produto": "$_id", "faturamento": 1, "volume_kg": 1}}
    ]
    items = await fatos.aggregate(pipeline).to_list(limit)
    return {"items": items}


# ---------- 2. Financeiro ----------
@router.get("/financeiro/faturamento-por-produto")
async def financeiro_faturamento_produto(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = Query(50, ge=5, le=200),
):
    """Faturamento por produto (barras)."""
    match = _match_fatos(date_from=date_from, date_to=date_to)
    pipeline = [
        {"$match": match},
        {"$addFields": {"receita_estimada": _receita_expr()}},
        {"$group": {"_id": "$tipo_produto", "faturamento": {"$sum": "$receita_estimada"}}},
        {"$sort": {"faturamento": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "produto": "$_id", "faturamento": 1}}
    ]
    items = await fatos.aggregate(pipeline).to_list(limit)
    return {"items": items}


@router.get("/financeiro/faturamento-por-canal")
async def financeiro_faturamento_canal(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = Query(50, ge=5, le=200),
):
    """Faturamento por canal."""
    match = _match_fatos(date_from=date_from, date_to=date_to)
    pipeline = [
        {"$match": match},
        {"$addFields": {"receita_estimada": _receita_expr()}},
        {"$group": {"_id": "$canal", "faturamento": {"$sum": "$receita_estimada"}}},
        {"$sort": {"faturamento": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "canal": "$_id", "faturamento": 1}}
    ]
    items = await fatos.aggregate(pipeline).to_list(limit)
    return {"items": items}


@router.get("/financeiro/faturamento-por-regiao")
async def financeiro_faturamento_regiao(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = Query(50, ge=5, le=200),
):
    """Faturamento por região."""
    match = _match_fatos(date_from=date_from, date_to=date_to)
    pipeline = [
        {"$match": match},
        {"$addFields": {"receita_estimada": _receita_expr()}},
        {"$group": {"_id": "$regiao_destino", "faturamento": {"$sum": "$receita_estimada"}}},
        {"$sort": {"faturamento": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "regiao": "$_id", "faturamento": 1}}
    ]
    items = await fatos.aggregate(pipeline).to_list(limit)
    return {"items": items}


@router.get("/financeiro/preco-medio-kg")
async def financeiro_preco_medio_kg(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    grupo: Optional[str] = Query(None, description="produto | canal | regiao"),
):
    """Preço médio por kg, opcionalmente por grupo (produto, canal ou regiao)."""
    match = _match_fatos(date_from=date_from, date_to=date_to)
    field = {"produto": "tipo_produto", "canal": "canal", "regiao": "regiao_destino"}.get(grupo or "") or None
    if field:
        pipeline = [
            {"$match": match},
            {"$group": {"_id": f"${field}", "preco_medio_kg": {"$avg": "$preco_unitario_brl_kg"}, "volume_kg": {"$sum": "$quantidade_kg"}}},
            {"$sort": {"preco_medio_kg": -1}},
            {"$project": {"_id": 0, "grupo": "$_id", "preco_medio_kg": 1, "volume_kg": 1}}
        ]
        items = await fatos.aggregate(pipeline).to_list(500)
        return {"agrupado_por": grupo, "items": items}
    pipeline = [
        {"$match": match},
        {"$group": {"_id": None, "preco_medio_kg": {"$avg": "$preco_unitario_brl_kg"}}},
        {"$project": {"_id": 0}}
    ]
    res = await fatos.aggregate(pipeline).to_list(1)
    return {"preco_medio_kg": res[0]["preco_medio_kg"] if res else None}


@router.get("/financeiro/evolucao-faturamento")
async def financeiro_evolucao_faturamento(
    granularity: str = Query("month", pattern="^(day|month)$"),
    meses: int = Query(12, ge=1, le=36),
):
    """Evolução de faturamento no tempo (linha)."""
    end = datetime.utcnow()
    start = end - timedelta(days=meses * 31)
    match = {"data_pedido": {"$gte": start, "$lte": end}}
    date_group = {"year": {"$year": "$data_pedido"}, "month": {"$month": "$data_pedido"}}
    if granularity == "day":
        date_group["day"] = {"$dayOfMonth": "$data_pedido"}
    pipeline = [
        {"$match": match},
        {"$addFields": {"receita_estimada": _receita_expr()}},
        {"$group": {"_id": date_group, "faturamento": {"$sum": "$receita_estimada"}}},
        {"$sort": {"_id.year": 1, "_id.month": 1, **({"_id.day": 1} if granularity == "day" else {})}},
        {"$project": {"_id": 0, "year": "$_id.year", "month": "$_id.month", **({"day": "$_id.day"} if granularity == "day" else {}), "faturamento": 1}}
    ]
    items = await fatos.aggregate(pipeline).to_list(10000)
    return {"granularity": granularity, "items": items}


@router.get("/financeiro/canal-produto-empilhado")
async def financeiro_canal_produto_empilhado(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit_produto: int = Query(15, ge=5, le=50),
):
    """Faturamento canal × produto (barras empilhadas). Retorna por canal com breakdown por produto."""
    match = _match_fatos(date_from=date_from, date_to=date_to)
    pipeline = [
        {"$match": match},
        {"$addFields": {"receita_estimada": _receita_expr()}},
        {"$group": {"_id": {"canal": "$canal", "produto": "$tipo_produto"}, "faturamento": {"$sum": "$receita_estimada"}}},
        {"$sort": {"_id.canal": 1, "faturamento": -1}},
        {"$project": {"_id": 0, "canal": "$_id.canal", "produto": "$_id.produto", "faturamento": 1}}
    ]
    items = await fatos.aggregate(pipeline).to_list(500)
    return {"items": items}


# ---------- 3. Vendas ----------
@router.get("/vendas/volume-por-canal")
async def vendas_volume_canal(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = Query(50, ge=5, le=200),
):
    """Volume (kg) por canal."""
    match = _match_fatos(date_from=date_from, date_to=date_to)
    pipeline = [
        {"$match": match},
        {"$group": {"_id": "$canal", "volume_kg": {"$sum": "$quantidade_kg"}, "num_pedidos": {"$sum": 1}}},
        {"$sort": {"volume_kg": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "canal": "$_id", "volume_kg": 1, "num_pedidos": 1}}
    ]
    items = await fatos.aggregate(pipeline).to_list(limit)
    return {"items": items}


@router.get("/vendas/mix-produtos")
async def vendas_mix_produtos(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = Query(30, ge=5, le=100),
):
    """Mix de produtos (volume e faturamento) para treemap/donut."""
    match = _match_fatos(date_from=date_from, date_to=date_to)
    pipeline = [
        {"$match": match},
        {"$addFields": {"receita_estimada": _receita_expr()}},
        {"$group": {"_id": "$tipo_produto", "volume_kg": {"$sum": "$quantidade_kg"}, "faturamento": {"$sum": "$receita_estimada"}, "num_pedidos": {"$sum": 1}}},
        {"$sort": {"volume_kg": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "produto": "$_id", "volume_kg": 1, "faturamento": 1, "num_pedidos": 1}}
    ]
    items = await fatos.aggregate(pipeline).to_list(limit)
    return {"items": items}


@router.get("/vendas/ranking-segmentos")
async def vendas_ranking_segmentos(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    ordenar_por: str = Query("faturamento", pattern="^(faturamento|volume_kg|num_pedidos)$"),
    limit: int = Query(30, ge=5, le=200),
):
    """Ranking de clientes/segmentos (faturamento, volume ou pedidos)."""
    match = _match_fatos(date_from=date_from, date_to=date_to)
    pipeline = [
        {"$match": match},
        {"$addFields": {"receita_estimada": _receita_expr()}},
        {"$group": {
            "_id": "$cliente_segmento",
            "faturamento": {"$sum": "$receita_estimada"},
            "volume_kg": {"$sum": "$quantidade_kg"},
            "num_pedidos": {"$sum": 1},
        }},
        {"$sort": {ordenar_por: -1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "segmento": "$_id", "faturamento": 1, "volume_kg": 1, "num_pedidos": 1}}
    ]
    items = await fatos.aggregate(pipeline).to_list(limit)
    return {"ordenar_por": ordenar_por, "items": items}


@router.get("/vendas/kpis")
async def vendas_kpis(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    """Volume total, nº pedidos, participação por canal (resumo)."""
    match = _match_fatos(date_from=date_from, date_to=date_to)
    total_pipeline = [
        {"$match": match},
        {"$group": {"_id": None, "volume_kg": {"$sum": "$quantidade_kg"}, "num_pedidos": {"$sum": 1}}},
        {"$project": {"_id": 0}}
    ]
    canal_pipeline = [
        {"$match": match},
        {"$group": {"_id": "$canal", "volume_kg": {"$sum": "$quantidade_kg"}, "num_pedidos": {"$sum": 1}}},
        {"$project": {"_id": 0, "canal": "$_id", "volume_kg": 1, "num_pedidos": 1}}
    ]
    total = await fatos.aggregate(total_pipeline).to_list(1)
    por_canal = await fatos.aggregate(canal_pipeline).to_list(50)
    tot = total[0] if total else {"volume_kg": 0, "num_pedidos": 0}
    for c in por_canal:
        c["participacao_volume_pct"] = round(c["volume_kg"] / tot["volume_kg"] * 100, 2) if tot["volume_kg"] else 0
        c["participacao_pedidos_pct"] = round(c["num_pedidos"] / tot["num_pedidos"] * 100, 2) if tot["num_pedidos"] else 0
    return {"totais": tot, "por_canal": por_canal}


# ---------- 4. Produtos ----------
@router.get("/produtos/comparativo-polpa-manteiga")
async def produtos_comparativo(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    """Polpa vs Manteiga: volume, faturamento e preço médio por tipo."""
    match = _match_fatos(date_from=date_from, date_to=date_to)
    pipeline = [
        {"$match": match},
        {"$addFields": {"receita_estimada": _receita_expr()}},
        {"$group": {
            "_id": "$tipo_produto",
            "volume_kg": {"$sum": "$quantidade_kg"},
            "faturamento": {"$sum": "$receita_estimada"},
            "num_pedidos": {"$sum": 1},
        }},
        {"$addFields": {"preco_medio_kg": {"$cond": [{"$eq": ["$volume_kg", 0]}, None, {"$divide": ["$faturamento", "$volume_kg"]}]}}},
        {"$sort": {"faturamento": -1}},
        {"$project": {"_id": 0, "produto": "$_id", "volume_kg": 1, "faturamento": 1, "num_pedidos": 1, "preco_medio_kg": 1}}
    ]
    items = await fatos.aggregate(pipeline).to_list(50)
    return {"items": items}


@router.get("/produtos/evolucao-mensal-por-produto")
async def produtos_evolucao_mensal(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    meses: int = Query(12, ge=1, le=36),
):
    """Evolução mensal de volume e faturamento por produto (linha por produto)."""
    if not date_from or not date_to:
        end = datetime.utcnow()
        start = end - timedelta(days=meses * 31)
        match = {"data_pedido": {"$gte": start, "$lte": end}}
    else:
        match = _match_fatos(date_from=date_from, date_to=date_to)
    pipeline = [
        {"$match": match},
        {"$addFields": {"receita_estimada": _receita_expr()}},
        {"$group": {
            "_id": {"year": {"$year": "$data_pedido"}, "month": {"$month": "$data_pedido"}, "produto": "$tipo_produto"},
            "volume_kg": {"$sum": "$quantidade_kg"},
            "faturamento": {"$sum": "$receita_estimada"},
        }},
        {"$sort": {"_id.year": 1, "_id.month": 1}},
        {"$project": {"_id": 0, "year": "$_id.year", "month": "$_id.month", "produto": "$_id.produto", "volume_kg": 1, "faturamento": 1}}
    ]
    items = await fatos.aggregate(pipeline).to_list(5000)
    return {"items": items}


# ---------- 5. Canais & Mercados ----------
@router.get("/canais-mercados/performance-canal")
async def canais_performance_canal(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = Query(30, ge=5, le=200),
):
    """Performance por canal (faturamento e volume)."""
    match = _match_fatos(date_from=date_from, date_to=date_to)
    pipeline = [
        {"$match": match},
        {"$addFields": {"receita_estimada": _receita_expr()}},
        {"$group": {"_id": "$canal", "faturamento": {"$sum": "$receita_estimada"}, "volume_kg": {"$sum": "$quantidade_kg"}, "num_pedidos": {"$sum": 1}}},
        {"$sort": {"faturamento": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "canal": "$_id", "faturamento": 1, "volume_kg": 1, "num_pedidos": 1}}
    ]
    items = await fatos.aggregate(pipeline).to_list(limit)
    return {"items": items}


@router.get("/canais-mercados/performance-regiao")
async def canais_performance_regiao(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = Query(50, ge=5, le=200),
):
    """Performance por região (Brasil × Exterior / regiões)."""
    match = _match_fatos(date_from=date_from, date_to=date_to)
    pipeline = [
        {"$match": match},
        {"$addFields": {"receita_estimada": _receita_expr()}},
        {"$group": {"_id": "$regiao_destino", "faturamento": {"$sum": "$receita_estimada"}, "volume_kg": {"$sum": "$quantidade_kg"}, "num_pedidos": {"$sum": 1}}},
        {"$sort": {"faturamento": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "regiao": "$_id", "faturamento": 1, "volume_kg": 1, "num_pedidos": 1}}
    ]
    items = await fatos.aggregate(pipeline).to_list(limit)
    return {"items": items}


# ---------- 6. Clientes ----------
@router.get("/clientes/por-segmento")
async def clientes_por_segmento(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = Query(30, ge=5, le=200),
):
    """Faturamento e volume por segmento de cliente; ticket médio por segmento."""
    match = _match_fatos(date_from=date_from, date_to=date_to)
    pipeline = [
        {"$match": match},
        {"$addFields": {"receita_estimada": _receita_expr()}},
        {"$group": {
            "_id": "$cliente_segmento",
            "faturamento": {"$sum": "$receita_estimada"},
            "volume_kg": {"$sum": "$quantidade_kg"},
            "num_pedidos": {"$sum": 1},
        }},
        {"$addFields": {"ticket_medio": {"$cond": [{"$eq": ["$num_pedidos", 0]}, None, {"$divide": ["$faturamento", "$num_pedidos"]}]}}},
        {"$sort": {"faturamento": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "segmento": "$_id", "faturamento": 1, "volume_kg": 1, "num_pedidos": 1, "ticket_medio": 1}}
    ]
    items = await fatos.aggregate(pipeline).to_list(limit)
    return {"items": items}


# ---------- 7. Qualidade & Satisfação ----------
@router.get("/qualidade-satisfacao/nps")
async def qualidade_nps(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    por_produto: bool = False,
):
    """NPS médio (global ou por produto). Tendência ao longo do tempo no timeseries abaixo."""
    match = _match_fatos(date_from=date_from, date_to=date_to)
    if por_produto:
        pipeline = [
            {"$match": match},
            {"$group": {"_id": "$tipo_produto", "nps_medio": {"$avg": "$nps_0a10"}, "num_avaliacoes": {"$sum": 1}}},
            {"$sort": {"nps_medio": -1}},
            {"$project": {"_id": 0, "produto": "$_id", "nps_medio": 1, "num_avaliacoes": 1}}
        ]
        items = await fatos.aggregate(pipeline).to_list(50)
        return {"por_produto": True, "items": items}
    pipeline = [
        {"$match": match},
        {"$group": {"_id": None, "nps_medio": {"$avg": "$nps_0a10"}, "num_avaliacoes": {"$sum": 1}}},
        {"$project": {"_id": 0}}
    ]
    res = await fatos.aggregate(pipeline).to_list(1)
    return {"nps_medio": res[0]["nps_medio"] if res else None, "num_avaliacoes": res[0].get("num_avaliacoes", 0) if res else 0}


@router.get("/qualidade-satisfacao/nps-serie")
async def qualidade_nps_serie(
    granularity: str = Query("month", pattern="^(day|month)$"),
    meses: int = Query(12, ge=1, le=36),
):
    """NPS ao longo do tempo (linha)."""
    end = datetime.utcnow()
    start = end - timedelta(days=meses * 31)
    match = {"data_pedido": {"$gte": start, "$lte": end}}
    date_group = {"year": {"$year": "$data_pedido"}, "month": {"$month": "$data_pedido"}}
    if granularity == "day":
        date_group["day"] = {"$dayOfMonth": "$data_pedido"}
    pipeline = [
        {"$match": match},
        {"$group": {"_id": date_group, "nps_medio": {"$avg": "$nps_0a10"}}},
        {"$sort": {"_id.year": 1, "_id.month": 1, **({"_id.day": 1} if granularity == "day" else {})}},
        {"$project": {"_id": 0, "year": "$_id.year", "month": "$_id.month", **({"day": "$_id.day"} if granularity == "day" else {}), "nps_medio": 1}}
    ]
    items = await fatos.aggregate(pipeline).to_list(10000)
    return {"granularity": granularity, "items": items}


@router.get("/qualidade-satisfacao/qualidade-por-produto")
async def qualidade_indice_por_produto(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    """Índice de qualidade (polpa) por produto - join fatos + polpa para pedidos de polpa."""
    match = _match_fatos(date_from=date_from, date_to=date_to)
    # Considerar apenas pedidos que têm polpa (tipo_produto contém Polpa)
    match["tipo_produto"] = {"$regex": "Polpa", "$options": "i"}
    pipeline = [
        {"$match": match},
        {"$lookup": {"from": "polpa_metricas", "localField": "id_pedido", "foreignField": "id_pedido", "as": "polpa"}},
        {"$unwind": "$polpa"},
        {"$group": {"_id": "$tipo_produto", "indice_qualidade_medio": {"$avg": "$polpa.indice_qualidade_1a10"}, "num_pedidos": {"$sum": 1}}},
        {"$sort": {"indice_qualidade_medio": -1}},
        {"$project": {"_id": 0, "produto": "$_id", "indice_qualidade_medio": 1, "num_pedidos": 1}}
    ]
    items = await fatos.aggregate(pipeline).to_list(50)
    return {"items": items}


# ---------- 8. Logística & Custos ----------
@router.get("/logistica-custos/resumo")
async def logistica_resumo(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    """Custo logístico médio, total e impacto. Dados da coleção polpa + fatos."""
    match_fatos = _match_fatos(date_from=date_from, date_to=date_to)
    match_fatos["tipo_produto"] = {"$regex": "Polpa", "$options": "i"}
    pipeline = [
        {"$match": match_fatos},
        {"$lookup": {"from": "polpa_metricas", "localField": "id_pedido", "foreignField": "id_pedido", "as": "polpa"}},
        {"$unwind": "$polpa"},
        {"$addFields": {"receita_estimada": _receita_expr()}},
        {"$group": {
            "_id": None,
            "custo_logistico_total": {"$sum": "$polpa.logistica_brl"},
            "receita_total": {"$sum": "$receita_estimada"},
            "num_pedidos": {"$sum": 1},
        }},
        {"$addFields": {
            "custo_logistico_medio": {"$cond": [{"$eq": ["$num_pedidos", 0]}, None, {"$divide": ["$custo_logistico_total", "$num_pedidos"]}]},
            "custo_vs_receita_pct": {"$cond": [
                {"$eq": ["$receita_total", 0]}, None,
                {"$multiply": [{"$divide": ["$custo_logistico_total", "$receita_total"]}, 100]}
            ]}
        }},
        {"$project": {"_id": 0}}
    ]
    res = await fatos.aggregate(pipeline).to_list(1)
    return res[0] if res else {"custo_logistico_total": 0, "custo_logistico_medio": None, "receita_total": 0, "custo_vs_receita_pct": None, "num_pedidos": 0}


@router.get("/logistica-custos/evolucao-custo")
async def logistica_evolucao_custo(
    granularity: str = Query("month", pattern="^(day|month)$"),
    meses: int = Query(12, ge=1, le=36),
):
    """Evolução do custo logístico ao longo do tempo (linha)."""
    end = datetime.utcnow()
    start = end - timedelta(days=meses * 31)
    match = {"data_pedido": {"$gte": start, "$lte": end}, "tipo_produto": {"$regex": "Polpa", "$options": "i"}}
    date_group = {"year": {"$year": "$data_pedido"}, "month": {"$month": "$data_pedido"}}
    if granularity == "day":
        date_group["day"] = {"$dayOfMonth": "$data_pedido"}
    pipeline = [
        {"$match": match},
        {"$lookup": {"from": "polpa_metricas", "localField": "id_pedido", "foreignField": "id_pedido", "as": "polpa"}},
        {"$unwind": "$polpa"},
        {"$group": {"_id": date_group, "custo_logistico": {"$sum": "$polpa.logistica_brl"}, "num_pedidos": {"$sum": 1}}},
        {"$sort": {"_id.year": 1, "_id.month": 1, **({"_id.day": 1} if granularity == "day" else {})}},
        {"$project": {"_id": 0, "year": "$_id.year", "month": "$_id.month", **({"day": "$_id.day"} if granularity == "day" else {}), "custo_logistico": 1, "num_pedidos": 1}}
    ]
    items = await fatos.aggregate(pipeline).to_list(10000)
    return {"granularity": granularity, "items": items}


@router.get("/logistica-custos/logistica-vs-volume")
async def logistica_vs_volume(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = Query(100, ge=10, le=500),
):
    """Scatter: custo logístico × volume (por pedido ou agregado). Por pedido para scatter."""
    match = _match_fatos(date_from=date_from, date_to=date_to)
    match["tipo_produto"] = {"$regex": "Polpa", "$options": "i"}
    pipeline = [
        {"$match": match},
        {"$lookup": {"from": "polpa_metricas", "localField": "id_pedido", "foreignField": "id_pedido", "as": "polpa"}},
        {"$unwind": "$polpa"},
        {"$project": {
            "_id": 0,
            "id_pedido": 1,
            "volume_kg": "$quantidade_kg",
            "custo_logistico": "$polpa.logistica_brl",
            "receita_estimada": {"$multiply": ["$quantidade_kg", "$preco_unitario_brl_kg"]}
        }},
        {"$limit": limit}
    ]
    items = await fatos.aggregate(pipeline).to_list(limit)
    return {"items": items}
