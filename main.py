import json
import logging
import os
import traceback
from datetime import datetime

from fastapi import FastAPI, Depends, Request, status, HTTPException

app = FastAPI()

logging.basicConfig(level=logging.INFO)

# Simulating JWT authentication dependency
def JWTAgent():
    def verify_token():
        return True
    return verify_token

# Simulating BigQuery client
class FakeBigQueryClient:
    def query(self, query_str, job_config=None):
        return [
            {"column1": "value1", "column2": "value2"},
            {"column1": "value3", "column2": "value4"}
        ]

client = FakeBigQueryClient()

@app.get("/")
def read_root():
    return {"message": "Hello, world!"}


@app.get("/analytics/api/v1/data_sources/{module}/{source_type}")
async def data_sources(module: str, source_type: str, jwt: bool = Depends(JWTAgent())):
    try:
        data = {
            module: {
                "source_type": source_type,
                "aggregations": ["hourly", "daily"],
                "columns": ["column1", "column2"],
                "queries": ["query1", "query2"]
            }
        }
        return data
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Exception: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analytics/api/v1/time_series/{module}/{datasource}")
async def time_series(
    request: Request, module: str, datasource: str, start_time: datetime, end_time: datetime, columns: str = "*"
):
    try:
        if start_time > end_time:
            raise HTTPException(status_code=400, detail="Start time must be before end time")
        
        query = f"SELECT {columns} FROM {module}.{datasource} WHERE start_time >= {start_time} AND end_time <= {end_time}"
        logging.info(f"Running query: {query}")
        
        rows = client.query(query)
        return {"data": rows}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
