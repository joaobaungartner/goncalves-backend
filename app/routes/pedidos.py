from fastapi import APIRouter, Query
from typing import Optional
from app.db import fatos, polpa, manteiga

router = APIRouter(prefix="/pedidos", tags=["pedidos"])

def build_match(
    tipo_produto: Optional[str],
    mes_do_ano_num: Optional[int],
    canal: Optional[str],
    regiao_destino: Optional[str],
    cliente_segmento: Optional[str],
):
    q = {}
    if tipo_produto: q["tipo_produto"] = tipo_produto
    if mes_do_ano_num: q["mes_do_ano_num"] = mes_do_ano_num
    if canal: q["canal"] = canal
    if regiao_destino: q["regiao_destino"] = regiao_destino
    if cliente_segmento: q["cliente_segmento"] = cliente_segmento
    return q

@router.get("")
async def listar_pedidos(
    tipo_produto: Optional[str] = None,
    mes_do_ano_num: Optional[int] = None,
    canal: Optional[str] = None,
    regiao_destino: Optional[str] = None,
    cliente_segmento: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    match = build_match(tipo_produto, mes_do_ano_num, canal, regiao_destino, cliente_segmento)

    skip = (page - 1) * page_size

    cursor = fatos.find(match, {"_id": 0}).sort("data_pedido", -1).skip(skip).limit(page_size)
    items = await cursor.to_list(length=page_size)
    total = await fatos.count_documents(match)

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "items": items
    }

@router.get("/kpis")
async def kpis(
    tipo_produto: Optional[str] = None,
    mes_do_ano_num: Optional[int] = None,
    canal: Optional[str] = None,
    regiao_destino: Optional[str] = None,
    cliente_segmento: Optional[str] = None,
):
    match = build_match(tipo_produto, mes_do_ano_num, canal, regiao_destino, cliente_segmento)

    pipeline = [
        {"$match": match},
        {"$addFields": {
            "receita_estimada": {"$multiply": ["$quantidade_kg", "$preco_unitario_brl_kg"]}
        }},
        {"$group": {
            "_id": None,
            "pedidos": {"$sum": 1},
            "volume_total_kg": {"$sum": "$quantidade_kg"},
            "receita_estimada_total": {"$sum": "$receita_estimada"},
            "preco_medio": {"$avg": "$preco_unitario_brl_kg"},
            "nps_medio": {"$avg": "$nps_0a10"},
        }},
        {"$project": {"_id": 0}}
    ]

    res = await fatos.aggregate(pipeline).to_list(length=1)
    return res[0] if res else {
        "pedidos": 0,
        "volume_total_kg": 0,
        "receita_estimada_total": 0,
        "preco_medio": None,
        "nps_medio": None,
    }

@router.get("/timeseries")
async def timeseries(
    tipo_produto: Optional[str] = None,
    mes_do_ano_num: Optional[int] = None,
    canal: Optional[str] = None,
    regiao_destino: Optional[str] = None,
    cliente_segmento: Optional[str] = None,
    granularity: str = Query("day", pattern="^(day|month)$"),
):
    match = build_match(tipo_produto, mes_do_ano_num, canal, regiao_destino, cliente_segmento)

    date_group = {
        "year": {"$year": "$data_pedido"},
        "month": {"$month": "$data_pedido"},
    }
    if granularity == "day":
        date_group["day"] = {"$dayOfMonth": "$data_pedido"}

    pipeline = [
        {"$match": match},
        {"$addFields": {"receita_estimada": {"$multiply": ["$quantidade_kg", "$preco_unitario_brl_kg"]}}},
        {"$group": {
            "_id": date_group,
            "volume_kg": {"$sum": "$quantidade_kg"},
            "receita_estimada": {"$sum": "$receita_estimada"},
            "nps_medio": {"$avg": "$nps_0a10"},
        }},
        {"$sort": {"_id.year": 1, "_id.month": 1, **({"_id.day": 1} if granularity == "day" else {})}},
        {"$project": {
            "_id": 0,
            "year": "$_id.year",
            "month": "$_id.month",
            **({"day": "$_id.day"} if granularity == "day" else {}),
            "volume_kg": 1,
            "receita_estimada": 1,
            "nps_medio": 1
        }}
    ]

    return await fatos.aggregate(pipeline).to_list(length=10000)

@router.get("/{id_pedido}")
async def detalhe_pedido(id_pedido: str):
    pedido = await fatos.find_one({"id_pedido": id_pedido}, {"_id": 0})
    if not pedido:
        return {"error": "Pedido não encontrado"}

    tipo = pedido.get("tipo_produto", "")

    detalhes = None
    if "Polpa" in tipo:
        detalhes = await polpa.find_one({"id_pedido": id_pedido}, {"_id": 0})
    elif "Manteiga" in tipo:
        detalhes = await manteiga.find_one({"id_pedido": id_pedido}, {"_id": 0})

    return {
        "pedido": pedido,
        "detalhes": detalhes
    }
