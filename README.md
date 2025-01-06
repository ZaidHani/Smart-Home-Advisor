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
 

### Tech Stack
+ Web Scraping: BeautifulSoup
+ Data Warehouse: PostgreSQL
+ Workflow Management: Apache Airflow
+ Machine Learning: PyCaret for automation, MLflow for tracking
+ Web Application FastAPI
+ Chatbot: Groq API with "llama-3.1-8b-instant" and "sentence-transformers/all-MiniLM-L6-v2" for embedding and FIASS as a vector database

### Data Pipeline Workflow
We have 2 data pipelines for this project, the first one is the initial data pipeline which will only run once and will pull all the properties data from OpenSooq, clean the data, and then store them in the data warehouse, and the second one is the incremental data pipeline which will run daily and pull only 1000 listing every day, clean them then store them, These pipelines are mostly similar, but there is a normalization method in the incremental pipeline to avoid primary key errors.

### Machine Learning Model

### FastAPI

### Contact
