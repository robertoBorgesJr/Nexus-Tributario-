from enum import Enum
from typing import Annotated, Any

from fastapi import APIRouter, Query

from api.db import catalog, get_cursor

router = APIRouter(prefix="/api/v1/alertas", tags=["Alertas"])


class Severidade(str, Enum):
    baixo = "BAIXO"
    medio = "MEDIO"
    alto = "ALTO"
    critico = "CRITICO"


def _build_filters(
    cnpj: str | None,
    competencia: str | None,
    severidade: Severidade | None,
    severidade_col: str,
) -> tuple[list[str], list]:
    filters, params = [], []
    if cnpj:
        filters.append("cnpj_empresa = ?")
        params.append(cnpj)
    if competencia:
        filters.append("competencia = ?")
        params.append(competencia)
    if severidade:
        filters.append(f"{severidade_col} = ?")
        params.append(severidade.value)
    return filters, params


def _rows_as_dicts(cursor) -> list[dict]:
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _query_alertas(
    table: str,
    cnpj: str | None,
    competencia: str | None,
    severidade: Severidade | None,
    severidade_col: str,
    limit: int,
    offset: int,
) -> dict[str, Any]:
    cat = catalog()
    filters, params = _build_filters(cnpj, competencia, severidade, severidade_col)
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


@router.get("/icms")
def alertas_icms(
    cnpj_empresa: Annotated[str | None, Query(description="CNPJ da empresa")] = None,
    competencia: Annotated[str | None, Query(description="Competência YYYYMM")] = None,
    severidade: Annotated[Severidade | None, Query(description="Filtrar por severidade")] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict[str, Any]:
    return _query_alertas(
        "alert_conciliacao_icms", cnpj_empresa, competencia, severidade, "severidade", limit, offset
    )


@router.get("/pis-cofins")
def alertas_pis_cofins(
    cnpj_empresa: Annotated[str | None, Query(description="CNPJ da empresa")] = None,
    competencia: Annotated[str | None, Query(description="Competência YYYYMM")] = None,
    severidade: Annotated[Severidade | None, Query(description="Filtrar por severidade geral")] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict[str, Any]:
    return _query_alertas(
        "alert_conciliacao_pis_cofins", cnpj_empresa, competencia, severidade, "severidade_geral", limit, offset
    )
