from typing import Annotated, Any

from fastapi import APIRouter, Query

from api.db import catalog, get_cursor

router = APIRouter(prefix="/api/v1", tags=["NF-e"])


@router.get("/nfe-itens")
def listar_nfe_itens(
    cnpj_empresa: Annotated[str | None, Query(description="CNPJ do emitente (14 dígitos)")] = None,
    competencia: Annotated[str | None, Query(description="Competência no formato YYYYMM")] = None,
    limit: Annotated[int, Query(ge=1, le=1000, description="Máximo de registros (padrão 100)")] = 100,
    offset: Annotated[int, Query(ge=0, description="Deslocamento para paginação")] = 0,
) -> dict[str, Any]:
    cat = catalog()
    filters, params = _build_filters(cnpj_empresa, competencia)
    where = f"WHERE {' AND '.join(filters)}" if filters else ""

    count_sql = f"SELECT COUNT(*) FROM {cat}.gold.fact_nfe_itens {where}"
    data_sql = (
        f"SELECT * FROM {cat}.gold.fact_nfe_itens {where} "
        f"ORDER BY competencia DESC, dt_emissao DESC "
        f"LIMIT {limit} OFFSET {offset}"
    )

    with get_cursor() as cur:
        cur.execute(count_sql, params)
        total = cur.fetchone()[0]
        cur.execute(data_sql, params)
        rows = _rows_as_dicts(cur)

    return {"data": rows, "total": total}


def _build_filters(cnpj: str | None, competencia: str | None) -> tuple[list[str], list]:
    filters, params = [], []
    if cnpj:
        filters.append("cnpj_emitente = ?")
        params.append(cnpj)
    if competencia:
        filters.append("competencia = ?")
        params.append(competencia)
    return filters, params


def _rows_as_dicts(cursor) -> list[dict]:
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]
