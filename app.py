from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
import tempfile
import os
import datetime
from bson import ObjectId
from pydantic import BaseModel
import uuid

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", default="CCSDS_DB")
HISTORY_COLLECTION_NAME = os.getenv("HISTORY_COLLECTION_NAME", default="CCSDS_History")
if not MONGO_URI:
    raise ValueError("MONGO_URI is not set in the .env file")

# MongoDB setup
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
history_collection = db[HISTORY_COLLECTION_NAME]

# FastAPI instance
app = FastAPI(title="Excel Structure Parser API")

@app.post("/uploadExcel")
async def upload_excel(file: UploadFile = File(...)):
    """Upload an Excel file and store the parsed structure in MongoDB."""
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported")

    # Save temporarily
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

        # Parse Excel file
        try:
            sheets = pd.read_excel(tmp_path, sheet_name=None)
            sheets_names_list = list(sheets.keys())
            structure = []

            for sheet_name in sheets_names_list:
                df = sheets[sheet_name]

                # Ensure required columns exist
                required_cols = ["نام فیلد", "فرمت", "نام متغیر", "تعداد بایت"]
                if not all(col in df.columns for col in required_cols):
                    raise ValueError(f"Missing required columns in sheet: {sheet_name}")

                field_name_col = df["نام فیلد"]
                variable_format_col = df["فرمت"]
                variable_name_col = df["نام متغیر"]
                number_of_bytes_col = df["تعداد بایت"]

                SID = {}
                is_first_SID = True
                SID_Number = "SID1"

                for j in range(len(field_name_col)):
                    if isinstance(field_name_col[j], str) and "SID" in field_name_col[j]:
                        SID_Number = field_name_col[j].split(":")[0]
                        if not is_first_SID:
                            structure.append(SID)
                            SID = {}
                        is_first_SID = False

                    if variable_name_col[j] != "نام متغیر" and not pd.isna(variable_name_col[j]):
                        SID["sub_system"] = sheet_name
                        SID["SID"] = SID_Number
                        variable_name = variable_name_col[j]
                        SID[variable_name] = "uint16_t" if pd.isna(variable_format_col[j]) else variable_format_col[j]

                structure.append(SID)

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error parsing Excel: {str(e)}")
        # Save to MongoDB
        try:
            COLLECTION_NAME = os.getenv("COLLECTION_NAME", default="CCSDS_Structure") + " " + str(
                datetime.datetime.now())
            collection = db[COLLECTION_NAME]
            collection.insert_many(structure)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"MongoDB Error: {str(e)}")

        try:
            history_collection.update_many({}, {"$set": {"is_current": False}})
            history_collection.insert_one({
                "collection_name": COLLECTION_NAME,
                "is_current": True,
            })
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"MongoDB Error in Modifying History Collection: {str(e)}")

        return JSONResponse(content={
            "message": "File processed successfully"
        })

@app.get("/getCurrentStructure")
async def get_current_structure():
    """Return all stored structures from MongoDB."""
    metadata = list(history_collection.find({"is_current": True}))
    if not metadata:
        return JSONResponse(content={"error": "No current structure found"}, status_code=404)

    # Access the collection referenced in metadata
    dataCollection = db[metadata[0]["collection_name"]]
    data = list(dataCollection.find({}))
    jsonable_data = bson_to_jsonable(data)
    return JSONResponse(content=jsonable_data)

@app.get("/getAllStructureMetadata")
async def get_all_structure_metadata():
    """Return name of all stored structures from MongoDB."""
    metadata = list(history_collection.find({}))
    if not metadata:
        return JSONResponse(content={"error": "No structure found"}, status_code=404)

    # Convert both to JSON-safe types
    jsonable_metadata = bson_to_jsonable(metadata)

    return JSONResponse(content=jsonable_metadata)


class StructureNameModel(BaseModel):
    structureName: str


@app.post("/getStructureByName")
async def get_structure_by_name(body: StructureNameModel):
    """Return a structure with specific id."""
    structure_name = body.structureName
    collection = db[structure_name]
    data = list(collection.find())
    if not data:
        return JSONResponse(content={"error": "No structure found"}, status_code=404)

    # Convert both to JSON-safe types
    jsonable_data = bson_to_jsonable(data)
    return JSONResponse(content=jsonable_data)


class StructureIdModel(BaseModel):
    structureId: str
@app.post("/changeCurrentStructure")
async def get_structure_by_name(body: StructureIdModel):
    """Change structure with specific id to current structure."""
    structure_id = body.structureId
    history_collection.update_many({}, {"$set": {"is_current": False}})
    history_collection.update_many({"_id": ObjectId(structure_id)}, {"$set": {"is_current": True}})
    return JSONResponse(content={
        "message": "Current structure changed successfully"
    })

def bson_to_jsonable(obj):
    """Recursively convert MongoDB BSON types into JSON-serializable types."""
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif isinstance(obj, list):
        return [bson_to_jsonable(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: bson_to_jsonable(value) for key, value in obj.items()}
    else:
        return obj