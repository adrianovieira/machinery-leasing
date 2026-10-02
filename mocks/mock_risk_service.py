from fastapi import FastAPI, Request


app = FastAPI(
    title="Mock de Serviço Externo de Análise de Risco",
    description="Serviço isolado externo para simulação de avaliação de risco financeiro.",
    version="1.0.0",
)


@app.get("/health", tags=["Health"])
def health_check():
    """Liveness probe do mock de risco."""
    return {"status": "UP"}


@app.post("/risk-analysis", tags=["Risk Analysis"])
async def evaluate_risk(request: Request):
    """Endpoint de análise de risco simulado.

    - Valores acima de 1.000.000 são reprovados.
    - Demais valores são aprovados.
    """
    data = await request.json()
    value = float(data.get("value", 0))

    if value > 1_000_000:
        return {
            "decision": "REJECTED",
            "reason": "Excede o limite máximo permitido para avaliação automática",
            "score": 450,
        }

    return {
        "decision": "APPROVED",
        "reason": "Perfil de crédito aprovado com sucesso",
        "score": 850,
    }


def main():
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)


if __name__ == "__main__":
    main()
