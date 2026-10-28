from typing import Annotated, Any

from fastapi import APIRouter, Query

from api.db import catalog, get_cursor

router = APIRouter(prefix="/api/v1", tags=["Apuração"])


def _build_filters(cnpj: str | None, competencia: str | None) -> tuple[list[str], list]:
    filters, params = [], []
    if cnpj:
        filters.append("cnpj_empresa = ?")
        params.append(cnpj)
    if competencia:
        filters.append("competencia = ?")
        params.append(competencia)
    return filters, params


def _rows_as_dicts(cursor) -> list[dict]:
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _query_fact(table: str, cnpj: str | None, competencia: str | None, limit: int, offset: int) -> dict[str, Any]:
    cat = catalog()
    filters, params = _build_filters(cnpj, competencia)
    where = f"WHERE {' AND '.join(filters)}" if filters else ""

    count_sql = f"SELECT COUNT(*) FROM {cat}.gold.{table} {where}"
    data_sql = (
        f"SELECT * FROM {cat}.gold.{table} {where} "
        f"ORDER BY competencia DESC "
        f"LIMIT {limit} OFFSET {offset}"
    )

    with get_cursor() as cur:
        cur.execute(count_sql, params)
        total = cur.fetchone()[0]
        cur.execute(data_sql, params)
        rows = _rows_as_dicts(cur)

    return {"data": rows, "total": total}


@router.get("/apuracao-icms")
def apuracao_icms(
    cnpj_empresa: Annotated[str | None, Query(description="CNPJ da empresa")] = None,
    competencia: Annotated[str | None, Query(description="Competência YYYYMM")] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict[str, Any]:
    return _query_fact("fact_apuracao_icms", cnpj_empresa, competencia, limit, offset)


@router.get("/apuracao-pis-cofins")
def apuracao_pis_cofins(
    cnpj_empresa: Annotated[str | None, Query(description="CNPJ da empresa")] = None,
    competencia: Annotated[str | None, Query(description="Competência YYYYMM")] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict[str, Any]:
    return _query_fact("fact_apuracao_pis_cofins", cnpj_empresa, competencia, limit, offset)
