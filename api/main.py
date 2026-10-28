from fastapi import FastAPI

from api.routers import alertas, apuracao, nfe

app = FastAPI(
    title="Nexus Tributário API",
    description="Acesso REST às gold tables do pipeline fiscal (NF-e, SPED, EFD Contribuições).",
    version="1.0.0",
)

app.include_router(nfe.router)
app.include_router(apuracao.router)
app.include_router(alertas.router)
