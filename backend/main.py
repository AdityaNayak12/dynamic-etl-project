from fastapi import FastAPI, UploadFile, File, HTTPException
import pandas as pd, json
from etl import handle_injestion
from db import schemas_col, batches_col

app = FastAPI(title = "Dynamic ETL Pipeline")

@app.get('/')

def home():
    return {"message":"Dynamic ETL is running"}

@app.post('/injest/json')

async def injest_json(file: UploadFile = File(...), entity: str = "default"):
    try:
        data = json.load(file.file)
        if not isinstance(data,list):
            raise HTTPException(status_code=400, detail="JSON must be list of records")
        version = handle_injestion(entity,data)
        return{"message": "Injested JSON Successfully","schema_version":version}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post('/injest/csv')
async def injest_csv(file: UploadFile = File(...), entity: str = "default"):
    try:
        df = pd.read_csv(file.file)
        records = df.to_dict(orient = 'records')
        version = handle_injestion(entity, records)
        return {"message":"Injested CSV successfully", "schema_version":version}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/schemas")
def get_schemas():
    all_schemas = list(schemas_col.find({},{"_id": 0}))
    return{"schemas":all_schemas}

@app.get("/batches")
def get_batches():
    batches = list(batches_col.find({},{"_id": 0}))
    return {'batches':batches}
