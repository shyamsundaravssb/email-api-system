from fastapi import FastAPI

app = FastAPI(title="Generation API")


@app.get("/health")
async def health():
    return {"status": "ok"}
