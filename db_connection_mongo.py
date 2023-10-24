#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #2
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
from pymongo import MongoClient
import datetime, string

def connectDataBase():
    # Create a database connection object using pymongo
    # --> add your Python code here
    DB_NAME = "corpus"
    DB_HOST = "localhost"
    DB_PORT = 27017

    try:
         client = MongoClient(host=DB_HOST, port=DB_PORT)
         db = client[DB_NAME]
         return db
    
    except:
         print("Database not connected successfully")

def createDocument(col, docId, docText, docTitle, docDate, docCat):
    terms_list = []
    tempText = docText
    #Remove punctuation
    for punctuation in string.punctuation:
        tempText = tempText.replace(punctuation, "")
    terms = tempText.lower().split()
    num_chars = len("".join(tempText.split()))

    #Create a term dict
    term_dict = {}
    for term in terms:
        if term in term_dict:
            term_dict[term] += 1
        else: 
            term_dict[term] = 1

    # Create list of terms
    for key, value in term_dict.items():
        temp = {"term": key,
                "count": value,
                "term_num_chars": len(key)}
        terms_list.append(temp)

    document = {
        "doc_id": docId,
        "text": docText,
        "title": docTitle,
        "num_chars": num_chars,
        "date": docDate,
        "category": docCat,
        "terms": terms_list
    }

    col.insert_one(document)

    # create a dictionary to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here

    # create a list of dictionaries to include term objects.
    # --> add your Python code here

    #Producing a final document as a dictionary including all the required document fields
    # --> add your Python code here

    # Insert the document
    # --> add your Python code here

def deleteDocument(col, docId):
    # Delete the document from the database
    # --> add your Python code here
    col.delete_one({"doc_id": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    # --> add your Python code here
    col.delete_one({"doc_id": docId})

    # Create the document with the same id
    # --> add your Python code here
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    documents = col.find({})
    
    term_dict = {}
    for document in documents:
        for term in document['terms']:
            if term['term'] not in term_dict:
                term_dict[term['term']] = document['title'] + ':' + str(term['count'])
            else:
                term_dict[term['term']] = term_dict[term['term']] + ', ' +  document['title'] + ':' + str(term['count'])

    return term_dict
