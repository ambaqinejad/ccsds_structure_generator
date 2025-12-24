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
from fastapi.middleware.cors import CORSMiddleware
import httpx


load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", default="mongodb://192.168.0.100:27017")
DB_NAME = os.getenv("DB_NAME", default="Parser")
HISTORY_COLLECTION_NAME = os.getenv("HISTORY_COLLECTION_NAME", default="CCSDS_History")
PARSER_SERVER_URL = os.getenv("PARSER_SERVER_URL", default="192.168.0.102:5000")
if not MONGO_URI:
    raise ValueError("MONGO_URI is not set in the .env file")

# MongoDB setup
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
history_collection = db[HISTORY_COLLECTION_NAME]

# FastAPI instance
app = FastAPI(title="Excel Structure Parser API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins (use specific domains in production)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def send_structure_update_notification_to_external_server():
    async with httpx.AsyncClient() as http_client:
        print(PARSER_SERVER_URL)
        response = await http_client.get(
            f"{PARSER_SERVER_URL}/updatePacketStructure",
            timeout=10.0
        )
        response.raise_for_status()
        return response.json()

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
                df.fillna("", inplace=True)

                # Ensure required columns exist
                required_cols = ["Field Name", "Type", "Variable Name", "Count", "Gain", "Offset", "Min", "Max", "Concept", "Unit"]
                if not all(col in df.columns for col in required_cols):
                    raise ValueError(f"Missing required columns in sheet: {sheet_name}")

                field_name_col = df[required_cols[0]]
                type_col = df[required_cols[1]]
                variable_name_col = df[required_cols[2]]
                count_col = df[required_cols[3]]
                gain_col = df[required_cols[4]]
                offset_col = df[required_cols[5]]
                min_col = df[required_cols[6]]
                max_col = df[required_cols[7]]
                concept_col = df[required_cols[8]]
                unit_col = df[required_cols[9]]

                _field_name = required_cols[0]
                _type = required_cols[1]
                _variable_name = required_cols[2]
                _count = required_cols[3]
                _gain = required_cols[4]
                _offset = required_cols[5]
                _min = required_cols[6]
                _max = required_cols[7]
                _concept = required_cols[8]
                _unit = required_cols[9]

                SID = {}
                is_first_SID = True
                SID_Full_Name = "SID1"
                last_valid_field_name = ""

                for j in range(len(field_name_col)):
                    if isinstance(field_name_col[j], str) and "SID" in field_name_col[j]:
                        SID_Full_Name = field_name_col[j]
                        if not is_first_SID:
                            structure.append(SID)
                            SID = {}
                        is_first_SID = False

                    if variable_name_col[j] != _variable_name and variable_name_col[j] != "":
                        SID_NUMBER = SID_Full_Name.split(":")[0].replace("SID", "")
                        SID_NUMBER = int(SID_NUMBER)
                        metadata = {"info": sheet_name, "full_name": SID_Full_Name, "SID": SID_Full_Name, "SIDNumber": SID_NUMBER}
                        if field_name_col[j] != "":
                            last_valid_field_name = field_name_col[j]

                        SID["metadata"] = metadata
                        sid_info_obj = {"field_name": field_name_col[j] if field_name_col[j] != "" else last_valid_field_name,
                                        "type": type_col[j] if type_col[j] != "" else "",
                                        "variable_name": variable_name_col[j] if variable_name_col[
                                                                                     j] != "" else "",
                                        "count": count_col[j] if count_col[j] != "" else "",
                                        "gain": gain_col[j] if gain_col[j] != "" else 1,
                                        "offset": offset_col[j] if offset_col[j] != "" else 0,
                                        "min": min_col[j] if min_col[j] != "" else "",
                                        "max": max_col[j] if max_col[j] != "" else "",
                                        "concept": concept_col[j] if concept_col[j] != "" else "",
                                        "unit": unit_col[j] if unit_col[j] != "" else ""}

                        SID[variable_name_col[j]] = sid_info_obj

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

        try:
            await send_structure_update_notification_to_external_server()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error in notifying structure change in parser server : {str(e)}")

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
    try:
        """Change structure with specific id to current structure."""
        structure_id = body.structureId
        history_collection.update_many({}, {"$set": {"is_current": False}})
        history_collection.update_many({"_id": ObjectId(structure_id)}, {"$set": {"is_current": True}})
        await send_structure_update_notification_to_external_server()
        return JSONResponse(content={
            "message": "Current structure changed successfully"
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")



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