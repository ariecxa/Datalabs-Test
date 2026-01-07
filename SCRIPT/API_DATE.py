from fastapi import FastAPI
from fastapi.responses import JSONResponse
from datetime import datetime
from zoneinfo import ZoneInfo

print('help')
app = FastAPI()

@app.get("/now")
def get_waktu_wib():
    wib_zone = ZoneInfo("Asia/Jakarta")  # WIB = UTC+7
    now_wib = datetime.now(wib_zone)
    return JSONResponse(content={"datetime_wib": now_wib.isoformat()})