from fastapi import FastAPI
from joblib import load

from pydantic import BaseModel, conlist

LABELS = ["got", "lotr", "hp"]

class Input(BaseModel):
    text: str

app = FastAPI(title="Classification pipeline", description="A simple text classifier", version="1.0")

model = load('classification_pipeline.joblib')

@app.post('/predict', tags=["predictions"])
async def get_prediction(incoming_data: Input):
    text = incoming_data.text
    class_index = model.predict([text])[0]
    label = LABELS[class_index]
    return {"label": label}