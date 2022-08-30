from fastapi import FastAPI
import random
from datetime import datetime

app = FastAPI()


@app.post("/run")
def run():
    dt = datetime.now()
    ts = datetime.timestamp(dt)
    random.seed(ts)
    return {"execution_id": random.randint(0, 100000000)}


@app.get("/runs/{execution_id}")
def runs(execution_id: int):
    return {"status": "finished"}
