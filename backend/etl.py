from genson import SchemaBuilder
from datetime import datetime
from db import schemas_col, batches_col, records_col

def infer_schema(records):
    builder = SchemaBuilder()
    for r in records:
        builder.add_object(r)
    return builder.to_schema()

def diff_schemas(old,new):
    old_fields = set(old.get("properties",{}))
    new_fields = set(new.get("properties",{}))
    add = list(new_fields-old_fields)
    removed = list(old_fields - new_fields)


def get_latest_schema(entity):
    return schemas_col.find_one({"entity":entity},sort=[("version",-1)])

def insert_schema(entity,schema,diff=None):
    lastest = get_latest_schema(entity)
    version = 1 if not latest else latest["version"]+1
    schemas_col.insert_one({
        "entity":entity,
        "version":version,
        "schema":schema,
        "diff":diff,
        "created_at":datetime.utcnow()
    })
    return version

def handle_injestion(entity, records):
    new_schema = infer_schema(records)
    latest = get_latest_schema(entity)

    if not latest:
        version = insert_schema(entity, new_schema)
    else:
        diff = diff_schemas(latest["schema"], new_schema)
        if diff["added"] or diff["removed"]:
            version = insert_schema(entity, new_schema, diff)
        else:
            version = latest["version"]

    batch = {
        "entity":entity,
        "version":version,
        "record_count":len(records),
        "created_at":datetime.utcnow()
    }

    batch_id = batches_col.insert_one(batch).inserted_id
    records_col.insert_many([{"batch_id":batch_id,"data":r} for r in records])
    return version
