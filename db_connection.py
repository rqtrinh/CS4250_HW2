#-------------------------------------------------------------------------
# AUTHOR: Ryan Trinh
# FILENAME: db_connection.py
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #1
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
import string
# --> add your Python code here
import psycopg2
from psycopg2.extras import RealDictCursor

def connectDataBase():

    DB_NAME = "corpus"
    DB_USER = "postgres"
    DB_PASS = "123"
    DB_HOST = "localhost"
    DB_PORT = "5432"

    try:
        conn = psycopg2.connect(database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT,
        cursor_factory=RealDictCursor)
        return conn
    except:
        print("Database not connected successfully")
    # Create a database connection object using psycopg2
    # --> add your Python code here

def createCategory(cur, catId, catName):
    sql = "Insert into category (id, name) Values (%s, %s)"

    cur.execute(sql, [catId, catName])
    # Insert a category in the database
    # --> add your Python code here


def createDocument(cur, docId, docText, docTitle, docDate, docCat):
    # 1 Get the category id based on the informed category name
    # --> add your Python code here
    sql = "Select id From Category Where name = %s"
    cur.execute(sql, [docCat])
    catId = cur.fetchone()
    catId = catId['id']

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    # --> add your Python code here
    
    #Get num_chars
    tempText = docText
    #Remove punctuation
    for punctuation in string.punctuation:
        tempText = tempText.replace(punctuation, "")
    num_chars = len("".join(tempText.split()))

    sql = "Insert into document (id, text, title, num_chars, date, cat_id) values(%s, %s, %s, %s, %s, %s)"
    cur.execute(sql, [docId, docText, docTitle, num_chars, docDate, catId])

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    terms = tempText.lower().split()
    sql = "Select count(*) from term where term = %s"

    for term in terms:
        # 3.2 For each term identified, check if the term already exists in the database
        cur.execute(sql, [term])
        count = cur.fetchone()
        if count['count'] > 0:
            continue
        # 3.3 In case the term does not exist, insert it into the database
        insert_term = "Insert into term (term, num_chars) values (%s, %s)"
        cur.execute(insert_term, [term, len(term)])

    # 4 Update the index
    # 4.1 Find all terms that belong to the document (Reusing terms from earlier)
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    count = {}
    for term in terms:
        if term in count:
            count[term] += 1
        else:
            count[term] = 1

    # 4.3 Insert the term and its corresponding count into the database
    insert_index = "Insert into index (term, doc_id, count) values (%s, %s, %s)"
    for key, value in count.items():
        cur.execute(insert_index, [key, docId, value])

def deleteDocument(cur, docId):
    # 1 Query the index based on the document to identify terms
    select_terms = "Select term from index where doc_id = %s"
    cur.execute(select_terms, [docId])
    terms = cur.fetchall()
    # 1.1 For each term identified, delete its occurrences in the index for that document
    delete_index = "Delete from index where term = %s and doc_id = %s"
    delete_term = "Delete from term where term = %s"

    count_term = "Select count(*) from index where term = %s"

    for term in terms:
        cur.execute(delete_index, [term['term'], docId])
        cur.execute(count_term, [term['term']])
        count = cur.fetchone()

        if count['count'] == 0:
            cur.execute(delete_term, [term['term']])

    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    # --> add your Python code here

    # 2 Delete the document from the database
    delete_doc = "Delete from document where id = %s"
    cur.execute(delete_doc, [docId])
    # --> add your Python code here

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):
    # 1 Delete the document
    # --> add your Python code here
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    # --> add your Python code here
    createDocument(cur, docId, docText, docTitle, docDate, docCat)

def getIndex(cur):
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    select_terms = "Select * from term"
    cur.execute(select_terms)
    terms = cur.fetchall()

    #Empty dictionary
    term_dict = {}
    for term in terms:
        select_title_term_count = "Select title, count From term inner Join Index on Term.term=Index.term Inner Join Document on Document.id=Index.doc_id Where Term.term = %s"
        cur.execute(select_title_term_count, [term['term']])
        
        title_term_count = cur.fetchall()
        term_value = ""

        counter = 0
        # Fetch titles and term counts
        for row in title_term_count:
            if counter == 0 :
                term_value += row['title'] + ':' + str(row['count'])
                counter += 1
            else:
                term_value += ', ' + row['title'] + ':' + str(row['count'])
        # Add the value to the term dict
        term_dict[term['term']] = term_value

    # Print the term dict
    return term_dict
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here