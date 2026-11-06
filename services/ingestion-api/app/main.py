from fastapi import FastAPI

app = FastAPI(title="Ingestion API")


@app.get("/health")
async def health():
    return {"status": "ok"}
