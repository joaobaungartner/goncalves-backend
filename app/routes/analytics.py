from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List, Literal, Dict, Any
from datetime import datetime
from app.db import db, fatos, polpa, manteiga
from app.analytics_config import COLLECTIONS, FIELDS, NUMERIC_FIELDS, CATEGORICAL_FIELDS

router = APIRouter(prefix="/analytics", tags=["analytics"])

COL_MAP = {
    "fatos": fatos,
    "polpa": polpa,
    "manteiga": manteiga,
}

Metric = Literal["count", "sum", "avg", "min", "max", "p50", "p90", "p95"]

def _get_collection(collection: str):
    if collection not in COL_MAP:
        raise HTTPException(400, f"collection inválida. Use: {list(COL_MAP.keys())}")
    return COL_MAP[collection]

def _validate_field(collection: str, field: str):
    if field not in FIELDS[collection]:
        raise HTTPException(400, f"field inválido para {collection}. Permitidos: {FIELDS[collection]}")

def _validate_fields(collection: str, fields: List[str]):
    for f in fields:
        _validate_field(collection, f)

def _parse_filters(
    collection: str,
    tipo_produto: Optional[str],
    mes_do_ano_num: Optional[int],
    canal: Optional[str],
    regiao_destino: Optional[str],
    cliente_segmento: Optional[str],
    date_from: Optional[str],
    date_to: Optional[str],
    extra_filters: Optional[str],  # formato: "campo=valor,campo2=valor2"
) -> Dict[str, Any]:
    q: Dict[str, Any] = {}

    # filtros padrão (só se existirem na coleção)
    if tipo_produto and "tipo_produto" in FIELDS[collection]:
        q["tipo_produto"] = tipo_produto
    if mes_do_ano_num is not None and "mes_do_ano_num" in FIELDS[collection]:
        q["mes_do_ano_num"] = mes_do_ano_num
    if canal and "canal" in FIELDS[collection]:
        q["canal"] = canal
    if regiao_destino and "regiao_destino" in FIELDS[collection]:
        q["regiao_destino"] = regiao_destino
    if cliente_segmento and "cliente_segmento" in FIELDS[collection]:
        q["cliente_segmento"] = cliente_segmento

    # intervalo de datas (somente para fatos)
    if "data_pedido" in FIELDS[collection] and (date_from or date_to):
        dt_filter = {}
        if date_from:
            dt_filter["$gte"] = datetime.fromisoformat(date_from)
        if date_to:
            dt_filter["$lte"] = datetime.fromisoformat(date_to)
        q["data_pedido"] = dt_filter

    # filtros extras: "campo=valor,campo2=valor2"
    if extra_filters:
        pairs = [p.strip() for p in extra_filters.split(",") if p.strip()]
        for pair in pairs:
            if "=" not in pair:
                raise HTTPException(400, "extra_filters inválido. Use: campo=valor,campo2=valor2")
            k, v = pair.split("=", 1)
            k = k.strip()
            v = v.strip()
            _validate_field(collection, k)
            # tenta converter números
            if k in NUMERIC_FIELDS.get(collection, []):
                try:
                    v = float(v) if "." in v else int(v)
                except:
                    pass
            q[k] = v

    return q


@router.get("/meta")
async def meta():
    """
    Retorna tudo que o front precisa saber:
    - coleções
    - campos por coleção
    - quais são numéricos/categóricos
    """
    return {
        "collections": list(COLLECTIONS.keys()),
        "fields": FIELDS,
        "numeric_fields": NUMERIC_FIELDS,
        "categorical_fields": CATEGORICAL_FIELDS,
    }


@router.get("/data")
async def data(
    collection: Literal["fatos", "polpa", "manteiga"],
    fields: Optional[str] = None,  # "campo1,campo2,campo3"
    tipo_produto: Optional[str] = None,
    mes_do_ano_num: Optional[int] = None,
    canal: Optional[str] = None,
    regiao_destino: Optional[str] = None,
    cliente_segmento: Optional[str] = None,
    date_from: Optional[str] = None,  # "2025-07-01T00:00:00"
    date_to: Optional[str] = None,
    extra_filters: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=2000),
):
    col = _get_collection(collection)
    q = _parse_filters(collection, tipo_produto, mes_do_ano_num, canal, regiao_destino, cliente_segmento, date_from, date_to, extra_filters)

    projection = {"_id": 0}
    if fields:
        f_list = [f.strip() for f in fields.split(",") if f.strip()]
        _validate_fields(collection, f_list)
        projection = {**projection, **{f: 1 for f in f_list}}

    skip = (page - 1) * page_size
    cursor = col.find(q, projection).skip(skip).limit(page_size)

    items = await cursor.to_list(length=page_size)
    total = await col.count_documents(q)

    return {"page": page, "page_size": page_size, "total": total, "items": items}


def _metric_expr(metric: str, field: Optional[str]):
    if metric == "count":
        return {"$sum": 1}

    if not field:
        raise HTTPException(400, "metric diferente de count exige field")

    if metric == "sum":
        return {"$sum": f"${field}"}
    if metric == "avg":
        return {"$avg": f"${field}"}
    if metric == "min":
        return {"$min": f"${field}"}
    if metric == "max":
        return {"$max": f"${field}"}

    # percentis (MongoDB >= 5.2 com $percentile em alguns ambientes)
    # fallback: usa aproximação com $setWindowFields seria mais pesado.
    # Aqui: usa $percentile se disponível no Atlas (geralmente sim).
    if metric in ("p50", "p90", "p95"):
        p = {"p50": 0.50, "p90": 0.90, "p95": 0.95}[metric]
        return {"$percentile": {"input": f"${field}", "p": [p], "method": "approximate"}}

    raise HTTPException(400, f"metric inválida: {metric}")


@router.get("/agg")
async def agg(
    collection: Literal["fatos", "polpa", "manteiga"],
    group_by: str = Query(..., description="Ex: canal ou regiao_destino. Para múltiplos: canal,regiao_destino"),
    metric: Metric = "count",
    field: Optional[str] = None,  # obrigatório se metric != count
    sort: Literal["asc", "desc"] = "desc",
    limit: int = Query(50, ge=1, le=5000),

    # filtros
    tipo_produto: Optional[str] = None,
    mes_do_ano_num: Optional[int] = None,
    canal: Optional[str] = None,
    regiao_destino: Optional[str] = None,
    cliente_segmento: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    extra_filters: Optional[str] = None,
):
    col = _get_collection(collection)

    gb = [g.strip() for g in group_by.split(",") if g.strip()]
    _validate_fields(collection, gb)

    if field:
        _validate_field(collection, field)

    q = _parse_filters(collection, tipo_produto, mes_do_ano_num, canal, regiao_destino, cliente_segmento, date_from, date_to, extra_filters)

    group_id = {g: f"${g}" for g in gb} if len(gb) > 1 else f"${gb[0]}"
    metric_expr = _metric_expr(metric, field)

    pipeline = [
        {"$match": q},
        {"$group": {"_id": group_id, "value": metric_expr}},
        {"$sort": {"value": 1 if sort == "asc" else -1}},
        {"$limit": limit},
    ]

    out = await col.aggregate(pipeline).to_list(length=limit)

    # normaliza _id em colunas
    items = []
    for r in out:
        if isinstance(r["_id"], dict):
            item = {**r["_id"], "value": r["value"]}
        else:
            item = {gb[0]: r["_id"], "value": r["value"]}
        # percentil vem como array [x]
        if isinstance(item["value"], list) and len(item["value"]) == 1:
            item["value"] = item["value"][0]
        items.append(item)

    return {"group_by": gb, "metric": metric, "field": field, "items": items}


@router.get("/dist")
async def dist(
    collection: Literal["fatos", "polpa", "manteiga"],
    field: str,
    kind: Literal["auto", "numeric", "categorical"] = "auto",
    bins: int = Query(20, ge=5, le=200),
    top_n: int = Query(30, ge=5, le=500),

    # filtros
    tipo_produto: Optional[str] = None,
    mes_do_ano_num: Optional[int] = None,
    canal: Optional[str] = None,
    regiao_destino: Optional[str] = None,
    cliente_segmento: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    extra_filters: Optional[str] = None,
):
    col = _get_collection(collection)
    _validate_field(collection, field)

    q = _parse_filters(collection, tipo_produto, mes_do_ano_num, canal, regiao_destino, cliente_segmento, date_from, date_to, extra_filters)

    is_num = field in NUMERIC_FIELDS.get(collection, [])
    if kind == "auto":
        kind = "numeric" if is_num else "categorical"

    if kind == "categorical":
        pipeline = [
            {"$match": q},
            {"$group": {"_id": f"${field}", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": top_n},
            {"$project": {"_id": 0, "label": "$_id", "count": 1}},
        ]
        items = await col.aggregate(pipeline).to_list(length=top_n)
        return {"field": field, "kind": "categorical", "items": items}

    # numeric histogram: usa $bucketAuto (auto bins)
    pipeline = [
        {"$match": {**q, field: {"$ne": None}}},
        {"$bucketAuto": {"groupBy": f"${field}", "buckets": bins, "output": {"count": {"$sum": 1}}}},
        {"$project": {"_id": 0, "min": "$_id.min", "max": "$_id.max", "count": 1}},
    ]
    items = await col.aggregate(pipeline).to_list(length=bins)
    return {"field": field, "kind": "numeric", "bins": bins, "items": items}


@router.get("/stats")
async def stats(
    collection: Literal["fatos", "polpa", "manteiga"],
    field: str,
    top_n: int = Query(20, ge=5, le=200),

    # filtros
    tipo_produto: Optional[str] = None,
    mes_do_ano_num: Optional[int] = None,
    canal: Optional[str] = None,
    regiao_destino: Optional[str] = None,
    cliente_segmento: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    extra_filters: Optional[str] = None,
):
    col = _get_collection(collection)
    _validate_field(collection, field)

    q = _parse_filters(collection, tipo_produto, mes_do_ano_num, canal, regiao_destino, cliente_segmento, date_from, date_to, extra_filters)

    is_num = field in NUMERIC_FIELDS.get(collection, [])
    if is_num:
        pipeline = [
            {"$match": {**q, field: {"$ne": None}}},
            {"$group": {
                "_id": None,
                "count": {"$sum": 1},
                "min": {"$min": f"${field}"},
                "max": {"$max": f"${field}"},
                "avg": {"$avg": f"${field}"},
                "std": {"$stdDevPop": f"${field}"},
            }},
            {"$project": {"_id": 0}}
        ]
        res = await col.aggregate(pipeline).to_list(length=1)
        return {"field": field, "type": "numeric", **(res[0] if res else {})}

    # categórico: cardinalidade + top
    pipeline_top = [
        {"$match": {**q, field: {"$ne": None}}},
        {"$group": {"_id": f"${field}", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": top_n},
        {"$project": {"_id": 0, "label": "$_id", "count": 1}},
    ]
    top = await col.aggregate(pipeline_top).to_list(length=top_n)
    distinct = await col.distinct(field, q)

    return {"field": field, "type": "categorical", "cardinality": len(distinct), "top": top}


@router.get("/timeseries")
async def timeseries(
    metric: Metric = "sum",
    field: str = Query(..., description="Campo numérico para somar/avg/etc"),
    granularity: Literal["day", "month"] = "day",

    # filtros (sempre aplicado sobre fatos)
    tipo_produto: Optional[str] = None,
    mes_do_ano_num: Optional[int] = None,
    canal: Optional[str] = None,
    regiao_destino: Optional[str] = None,
    cliente_segmento: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    extra_filters: Optional[str] = None,
):
    # Série temporal faz sentido em fatos_pedidos
    collection = "fatos"
    col = _get_collection(collection)
    _validate_field(collection, field)

    if field not in NUMERIC_FIELDS["fatos"]:
        raise HTTPException(400, f"field precisa ser numérico em fatos. Permitidos: {NUMERIC_FIELDS['fatos']}")

    q = _parse_filters(collection, tipo_produto, mes_do_ano_num, canal, regiao_destino, cliente_segmento, date_from, date_to, extra_filters)

    date_group = {"year": {"$year": "$data_pedido"}, "month": {"$month": "$data_pedido"}}
    if granularity == "day":
        date_group["day"] = {"$dayOfMonth": "$data_pedido"}

    metric_expr = _metric_expr(metric, field)

    pipeline = [
        {"$match": {**q, "data_pedido": {"$ne": None}}},
        {"$group": {"_id": date_group, "value": metric_expr}},
        {"$sort": {"_id.year": 1, "_id.month": 1, **({"_id.day": 1} if granularity == "day" else {})}},
        {"$project": {
            "_id": 0,
            "year": "$_id.year",
            "month": "$_id.month",
            **({"day": "$_id.day"} if granularity == "day" else {}),
            "value": 1
        }}
    ]
    out = await col.aggregate(pipeline).to_list(length=10000)

    # percentil vem como array [x]
    for r in out:
        if isinstance(r["value"], list) and len(r["value"]) == 1:
            r["value"] = r["value"][0]

    return {"field": field, "metric": metric, "granularity": granularity, "items": out}


@router.get("/join/{id_pedido}")
async def join(id_pedido: str):
    """
    Retorna o documento completo (fatos + técnica) baseado no tipo_produto.
    """
    pedido = await fatos.find_one({"id_pedido": id_pedido}, {"_id": 0})
    if not pedido:
        raise HTTPException(404, "Pedido não encontrado")

    tipo = pedido.get("tipo_produto", "")
    detalhes = None

    if "Polpa" in tipo:
        detalhes = await polpa.find_one({"id_pedido": id_pedido}, {"_id": 0})
    elif "Manteiga" in tipo:
        detalhes = await manteiga.find_one({"id_pedido": id_pedido}, {"_id": 0})

    return {"pedido": pedido, "detalhes": detalhes}
