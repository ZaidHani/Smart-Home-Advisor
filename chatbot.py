from groq import Groq
import os
from sentence_transformers import SentenceTransformer
import faiss
import psycopg2

# Initialize Groq client

api_key = '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'
client = Groq(api_key=api_key)

# ------------------------------------------------------------------------------------------------------------------------------------------------
# # Function to extract text from PDF
# def extract_text_from_pdf(pdf_path):
#     doc = fitz.open(pdf_path)
#     text = ""
#     for page_num in range(len(doc)):
#         page = doc.load_page(page_num)
#         text += page.get_text()
#     return text

# # Extract text from your PDF
# pdf_text = extract_text_from_pdf("Selected_Pages_92_100.pdf")

# # Document corpus (initially empty or with some documents)
# documents = []
# documents.append(pdf_text)  # Add extracted PDF text to documents
# ------------------------------------------------------------------------------------------------------------------------------------------------
def extract_text_from_postgres(db_config, query):
    """
    Extract text data from a PostgreSQL database based on a query.

    Parameters:
        db_config (dict): Database configuration containing host, database, user, password, and port.
        query (str): SQL query to fetch the text data.

    Returns:
        str: Combined text data from the query result.
    """
    documents = []
    text = ""
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Execute the query
        cursor.execute(query)
        rows = cursor.fetchall()
        index = 0
        # Combine all text from the rows
        for row in rows:
            row_text = " ".join(str(col) for col in row)  # Concatenate all column values
            text += row_text + "\n"  # Add a newline between rows for readability
            if index%100==0:
                documents.append(text)
                text = ''
        # Close the connection
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
    
    return documents

# Database configuration
db_config = {
    "host": "localhost",
    "database": "houses",
    "user": "postgres",
    "password": "2003"
    }

# SQL query to fetch text data
query = '''
        SELECT title, description, price, subcategory, area, 
        nearby, city, neighborhood,
        floor, furnished, number_of_floors
        FROM FACT_LISTING F 
        LEFT JOIN DIM_LOCATION DL ON F.LOCATION_ID=DL.LOCATION_ID
        LEFT JOIN DIM_PROPERTY DP ON F.PROPERTY_ID=DP.PROPERTY_ID
        LEFT JOIN DIM_PROPERTY_DETAILS DPD ON DP.DETAILS_ID=DPD.DETAILS_ID;
        '''

documents = extract_text_from_postgres(db_config, query)

# print(documents)

# Encode documents using a pre-trained sentence transformer model
retriever_model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
doc_embeddings = retriever_model.encode(documents, convert_to_tensor=True)

# Build the FAISS index
index = faiss.IndexFlatL2(doc_embeddings.shape[1])
index.add(doc_embeddings.cpu().numpy())

# Function to retrieve relevant documents
def retrieve_documents(query, top_k=5):
    query_embedding = retriever_model.encode([query], convert_to_tensor=True)
    _, doc_indices = index.search(query_embedding.cpu().numpy(), top_k)
    retrieved_docs = [documents[i] for i in doc_indices[0]]
    return retrieved_docs

# Function to generate response
def generate_response(query, retrieved_docs):
    print(retrieved_docs)
    input_text = 'Answer This Question: ' + query + 'Only depending on this text, if the text is not relevant do not answer, also do not mention that you are getting your information from a text and say its based on what you currently know, also speak in a jordanian accent' + " " + " ".join(retrieved_docs)
    completion = client.chat.completions.create(
        model="llama-3.1-70b-versatile",
        messages=[
            {
                "role": "system",
                "content": input_text
            }
        ],
        temperature=0,
        max_tokens=1024,
        top_p=1,
        stream=True,
        stop=None,
    )
    
    response = ""
    for chunk in completion:
        response += chunk.choices[0].delta.content or ""
    return response

# Function to combine retrieval and generation
def retrieve_and_generate(query, top_k=5):
    retrieved_docs = retrieve_documents(query, top_k)
    response = generate_response(query, retrieved_docs)
    return response

# Example usage
query = "طب اسمع، انا تزوجت جديد وبدي اشتري شقة على قدي الي وللمدام فاذا بتعرف شقق اسعارها منيحة في ضاحية الاستقلال لا تقصر، ويا ريت تراعي ميزانيتي يخوي لاني طفرت بعد العرس"
response = retrieve_and_generate(query)
print(response)