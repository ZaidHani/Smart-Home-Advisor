# Smart Home Advisor

### Tables of Contents
1. [Overview](https://github.com/ZaidHani/Smart-Home-Advisor/blob/main/README.md#overview:~:text=Overview)
2. [Features](https://github.com/ZaidHani/Smart-Home-Advisor/blob/main/README.md#features:~:text=Features)
3. [Installation and Setup](https://github.com/ZaidHani/Smart-Home-Advisor/blob/main/README.md#Setup:~:text=Setup)
4. [Tech Stack](https://github.com/ZaidHani/Smart-Home-Advisor/blob/main/README.md#tech-stack:~:text=Tech-Stack)
5. [Data Pipelines](https://github.com/ZaidHani/Smart-Home-Advisor/blob/main/README.md#Data-Pipelines:~:text=Data-Pipelines)
6. [Machine Learning Workflow](https://github.com/ZaidHani/Smart-Home-Advisor/blob/main/README.md#Machine-Learning-Workflow:~:text=Machine-Learning-Workflow)
7. [FastAPI Web Application](https://github.com/ZaidHani/Smart-Home-Advisor/blob/main/README.md#FastAPI-Web-Application:~:text=FastAPI-Web-Application)

### Overview
Smart Home Advisor is a comprehensive A-Z data Science project that provides insights into house listings from OpenSooq, descriptive analytics, and a chatbot interface for user interaction through a FastAPI-based web application.

![Full Project](https://github.com/ZaidHani/Smart-Home-Advisor/blob/main/images/All%20Project.png)

### Features
+ Automated data ingestion pipeline with Apache Airflow
+ Incremental updates to ensure data freshness
+ Machine learning pipeline automated with PyCaret and MLflow for tracking model improvements
+ Interactive web application built with FastAPI:
  + Home Page: Overview of the project
  + Models Page: Predict property prices
  + Dashboard Page: Visualize insights and trends
  + Chatbot Page: Interact with a RAG chatbot

 ### Setup 
 + Prerequisites:
   + WSL2
   + Docker
+ Airflow (all steps using wsl2):
  + Create a directory named 'airflow'
  + Download the docker-compose file from this [link](https://github.com/ZaidHani/Smart-Home-Advisor/blob/main/airflow/docker-compose.yaml) and move it inside the airflow directory
  + Run the following commands inside the airflow directory to create subdirectories  ```mkdir -p ./dags ./logs ./plugins ./config```
  ```echo -e "AIRFLOW_UID=$(id -u)" > .env``` ```AIRFLOW_UID=50000```
  + Run this command ```docker compose up airflow-init``` to initialize the airflow database
  + Finally run ```docker compose up``` or ```docker compose up -d``` to run airflow
   

### Tech Stack
+ Web Scraping: BeautifulSoup
+ Data Warehouse: PostgreSQL
+ Workflow Management: Apache Airflow
+ Machine Learning: PyCaret for automation, MLflow for tracking
+ Web Application FastAPI
+ Chatbot: Groq API with "llama-3.1-8b-instant" and "sentence-transformers/all-MiniLM-L6-v2" for embedding and FIASS as a vector database

### Data Pipeline Workflow
We have 2 data pipelines for this project, the first one is the initial data pipeline which will only run once and will pull all the properties data from OpenSooq, clean the data, and then store them in the data warehouse, and the second one is the incremental data pipeline which will run daily and pull only 1000 listing every day, clean them then store them, These pipelines are mostly similar, but there is a normalization method in the incremental pipeline to avoid primary key errors.

#### The Initial Data Pipeline
![Initial Pipeline](https://github.com/ZaidHani/Smart-Home-Advisor/blob/main/images/ini.png)
#### The Incremental Data Pipeline
![Incremental Pipeline](https://github.com/ZaidHani/Smart-Home-Advisor/blob/main/images/inc.png)
#### The Data Warehouse Schema
![Data Warehouse Schema](https://github.com/ZaidHani/Smart-Home-Advisor/blob/main/images/data%20warehouse%20postgre.jpg)

### Machine Learning Model
To develop machine learning models, we utilized PyCaret, an automated machine learning framework that constructs numerous data pipelines and identifies the model with the highest efficiency.
![PyCaret](https://github.com/ZaidHani/Smart-Home-Advisor/blob/main/images/pycaret%20logo.png)

### FastAPI
![Home Page](https://github.com/ZaidHani/Smart-Home-Advisor/blob/main/images/home%20page.png)
The web app is composed of 4 pages:
+ Home Page: Overview of the project
+ Models Page: Predict property prices
+ Dashboard Page: Visualize insights and trends
+ Chatbot Page: Interact with a RAG chatbot

### Chatbot
The chatbot was built using groq API with the "llama-3.1-70b-versatile" model and FIASS as a vector database.


### Conclusion
Smart Home Advisor is a data science project that analyzes the property market and provides a web app to help users buy properties and discover housing market trends.

### Contact
This project was done by:
[Zaid Allawanseh](https://www.linkedin.com/in/zaid-allawanseh/) ||
[Mohammad Aljermy](https://www.linkedin.com/in/mohammad-aljermy/) ||
[Mahmoud Ayman](https://www.linkedin.com/in/mahmoud-ayman3/)
