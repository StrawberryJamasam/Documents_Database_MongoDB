#-------------------------------------------------------------------------
# AUTHOR: Jane Barnett
# FILENAME: db_connection_mongo
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #3
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
from pymongo import MongoClient
from collections import Counter
import pprint

def connectDataBase():

    # Create a database connection object using pymongo
    
    DB_NAME = "CPP_A3_DOCUMENTS"
    DB_HOST = "localhost"
    DB_PORT = 27017
    
    try:
        
        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]

        return db
    
    except:
        print("Database not connected successfully")

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    text_dict = Counter((docText.lower()).split())

    # create a list of objects to include full term objects. [{"term", count, num_char}]
    terms = []
    
    for x in text_dict:
        terms.append({"term": x, "term_count": text_dict[x], "num_chars": len(x)})

    # produce a final document as a dictionary including all the required document fields
    document = {"_id": docId,
                "text": docText,
                "title": docTitle,
                "num_chars": sum(map(len, docText.split())),
                "date": docDate,
                "category": docCat,
                "terms": terms
                }
    
    # insert the document
    col.insert_one(document)

def deleteDocument(col, docId):

    # Delete the document from the database
    col.delete_one({"_id": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    deleteDocument(col, docId)

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    pipeline = [
        {"$unwind": {"path": "$terms"}},
        {"$group": {"_id": "$terms.term", "titles": { "$addToSet": "$title"}}}
    ]   
    
    pprint.pprint(list(col.aggregate(pipeline)))