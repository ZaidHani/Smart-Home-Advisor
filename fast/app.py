'''from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates # i will use jinja2 as templates and access whats inside the templates folder 
from fastapi.staticfiles import StaticFiles # i used this for css in the static folder 
import os
import joblib
import numpy as np
import pandas as pd
from fastapi import Form
model = joblib.load(r'C:/Users/4t4/OneDrive/final grad project/airflow/models/land-model.pkl')

# down here i created the app
app = FastAPI()

# now lets set the paths for the html and the css 
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request}) #TemplateResponse to render the index.html

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/ml-model", response_class=HTMLResponse)
async def ml_model_page(request: Request):
    return templates.TemplateResponse("ml_model.html", {"request": request})

@app.post("/predict")
async def predict(
    request: Request,
    long: float = Form(...),
    lat: float = Form(...),
    city: str = Form(...),
    neighborhood: str = Form(...),
    area: float = Form(...),
    zoned_for: str = Form(...)
):
    try:
        # Prepare input for prediction
        input_data = pd.DataFrame({
            'long': [long],
            'lat': [lat],
            'city': [city],
            'neighborhood': [neighborhood],
            'area': [area],
            'zoned_for': [zoned_for]
        })
        
        # Make prediction
        prediction = model.predict(input_data)[0]
        
        return templates.TemplateResponse("ml_model.html", {
            "request": request, 
            "prediction": f"${prediction:,.2f}",
            "input_long": long,
            "input_lat": lat,
            "input_city": city,
            "input_neighborhood": neighborhood,
            "input_area": area,
            "input_zoned_for": zoned_for
        })
    except Exception as e:
        print(f"Prediction Error: {e}")
        return templates.TemplateResponse("ml_model.html", {
            "request": request, 
            "error": str(e)
        })

@app.get("/chatbot", response_class=HTMLResponse)
async def chatbot(request: Request):
    return templates.TemplateResponse("chatbot.html", {"request": request})'''
#main page 
import os
import joblib
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from chatbot.chatbot import retrieve_and_generate
from groq import Groq
import os
from sentence_transformers import SentenceTransformer
import faiss
import psycopg2


app = FastAPI()

# Define base directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Mount static files and set up templates
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# Load models using joblib with full paths
try:
    property_model = joblib.load(r"C:/Users/4t4/OneDrive/final grad project/airflow/models/prop-model.pkl")
    land_model = joblib.load(r'C:/Users/4t4/OneDrive/final grad project/airflow/models/land-model.pkl')
except Exception as e:
    print(f"Error loading models: {e}")
    property_model = land_model = None

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/models", response_class=HTMLResponse)
async def models_page(request: Request):
    return templates.TemplateResponse("models.html", {"request": request})

import pandas as pd

@app.post("/predict-property", response_class=HTMLResponse)
async def predict_property(
    request: Request,
    long: float = Form(...),
    lat: float = Form(...),
    city: str = Form(...),
    neighborhood: str = Form(...),
    area: float = Form(...),
    subcategory: str = Form(...),
    facade: str = Form(...),
    bedrooms: int = Form(...),
    bathrooms: int = Form(...),
    furnished: str = Form(...),
    floor: int = Form(...),
    building_age: int = Form(...)
):
    """
    Predict property price using the property model.
    """
    try:
        # Prepare input DataFrame with the required features
        input_data = pd.DataFrame([{
            'long': long,
            'lat': lat,
            'city': city,
            'neighborhood': neighborhood,
            'area': area,
            'subcategory': subcategory,
            'facade': facade,
            'bedrooms': bedrooms,
            'bathrooms': bathrooms,
            'furnished': furnished,
            'floor': floor,
            'building_age': building_age
        }])

        # Predict the property price
        prediction = property_model.predict(input_data)[0]
        prediction_message = f"Predicted Property Price: ${prediction:,.2f}"
    except Exception as e:
        prediction_message = f"Prediction Error: {str(e)}"

    return templates.TemplateResponse(
        "models.html", {"request": request, "property_prediction": prediction_message}
    )


@app.post("/predict-land", response_class=HTMLResponse)
async def predict_land(
    request: Request,
    long: float = Form(...),
    lat: float = Form(...),
    city: str = Form(...),
    neighborhood: str = Form(...),
    area: float = Form(...),
    zoned_for: str = Form(...)
):
    """
    Predict land price using the land model.
    """
    try:
        # Prepare input DataFrame with the required features
        input_data = pd.DataFrame([{
            'long': long,
            'lat': lat,
            'city': city,
            'neighborhood': neighborhood,
            'area': area,
            'zoned_for': zoned_for
        }])

        # Predict the land price
        prediction = land_model.predict(input_data)[0]
        prediction_message = f"Predicted Land Price: ${prediction:,.2f}"
    except Exception as e:
        prediction_message = f"Prediction Error: {str(e)}"

    return templates.TemplateResponse(
        "models.html", {"request": request, "land_prediction": prediction_message}
    )

@app.get("/chatbot", response_class=HTMLResponse)
async def chatbot(request: Request):
    return templates.TemplateResponse("chatbot.html", {"request": request})

# Route to handle form submission and display chatbot response
@app.post("/chat", response_class=HTMLResponse)
async def chat(request: Request, message: str = Form(...)):
    try:
        # Call the function from the chatbot module
        bot_response = retrieve_and_generate(message)
    except Exception as e:
        bot_response = f"Error: {e}"
    
    # Return the template with both the user's message and the chatbot's response
    return templates.TemplateResponse(
        "chatbot.html", 
        {"request": request, "user_message": message, "bot_response": bot_response}
    )

    
