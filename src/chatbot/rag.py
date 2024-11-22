from langchain_groq import ChatGroq
from langchain.vectorstores import Chroma
from langchain.embeddings import OllamaEmbeddings
from langchain_community.document_loaders import SQLDatabaseLoader
from langchain_community.utilities.sql_database import SQLDatabase
import pandas as pd
from sqlalchemy import create_engine

from langchain.prompts import ChatPromptTemplate
from langchain_core.prompts import MessagesPlaceholder
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain.chains import create_history_aware_retriever, create_retrieval_chain
from langchain.schema import HumanMessage, AIMessage

api_key = "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"
engine = create_engine('postgresql://postgres:anon@localhost:5432/houses')
model="llama-3.1-70b-versatile"

query = '''
        SELECT *
        FROM FACT_LISTING F 
        LEFT JOIN DIM_LOCATION DL ON F.LOCATION_ID=DL.LOCATION_ID
        LEFT JOIN DIM_DATE DD ON F.DATE_ID=DD.DATE_ID
        LEFT JOIN DIM_PROPERTY DP ON F.PROPERTY_ID=DP.PROPERTY_ID
        LEFT JOIN DIM_PROPERTY_DETAILS DPD ON DP.DETAILS_ID=DPD.DETAILS_ID;
        '''

langchain_sql_engine = SQLDatabase(engine=engine)
loader = SQLDatabaseLoader(query=query,db=langchain_sql_engine)
result = loader.load_and_split()

embed = OllamaEmbeddings(model='llama3')

vector_store = Chroma(
    collection_name='vector_houses_data',
    embedding_function=embed
    )

vector_store.add_documents(result)
retriever = vector_store.as_retriever()

llm = ChatGroq(model=model, api_key=api_key)

system_message =  "Your name is Baybotty, you are a helpful assistant that helps the user in their queries related to real-estates. You will answer the user depending on the context. context: {context}"

prompt = ChatPromptTemplate.from_messages([
    ('system', system_message),
    MessagesPlaceholder(variable_name='context'),
    ('user', '{input}')
])

retriever_chain_prompt = ChatPromptTemplate.from_messages([
    MessagesPlaceholder(variable_name='chat_history'),
    ('user','{input}'),
    ('user', 'given the above conversation, generate a search qeury to look up in order to get information relevant to the conversation')
])


document_chain = create_stuff_documents_chain(llm, prompt)

# this is considred a runnable so we insert it into the create_retrieval_chain...
# there are another way to do this but let's just use this one...
retriever_chain = create_history_aware_retriever(llm, retriever, retriever_chain_prompt)

# we have used the documents chain as a combine_docs_chain...
# it is also considred a runnable...
conversational_retriever_chain = create_retrieval_chain(retriever=retriever_chain, combine_docs_chain=document_chain)

while True:
    final_message = input()
    chat_history = []
    output = conversational_retriever_chain.invoke({
                    'chat_history': chat_history,
                    'input': final_message
                })['answer']
    chat_history.append(HumanMessage(content=final_message))
    chat_history.append(AIMessage(content=output))
    print(output)