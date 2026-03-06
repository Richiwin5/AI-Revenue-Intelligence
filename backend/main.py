from fastapi import FastAPI

app = FastAPI(title="Tax Geospatial Intelligence")

@app.get("/")
def home():
    return {"status": "Project Running Successfully"}