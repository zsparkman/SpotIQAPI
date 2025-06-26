
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class Message(BaseModel):
    sender: str
    content: str

@app.get("/")
def read_root():
    return {"message": "SpotIQ API is live"}

@app.post("/process")
def process_message(message: Message):
    return {"response": f"Received message from {message.sender}: {message.content}"}
