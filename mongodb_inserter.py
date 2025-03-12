import csv
from pymongo import MongoClient

def insert_data():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["library"]

    books_collection = db["books"]
    borrowers_collection = db["borrowers"]
    transactions_collection = db["transactions"]

    # Insert Books
    with open("books.csv", "r") as csvfile:
        next(csvfile)  # Skip header
        reader = csv.reader(csvfile)
        books = []
        for row in reader:
            books.append({
                "book_id": int(row[0]),
                "title": row[1],
                "author": row[2],
                "year": int(row[3]),
                "genre": row[4]
            })
        if books:
            books_collection.insert_many(books)

    # Insert Borrowers
    with open("borrowers.csv", "r") as csvfile:
        next(csvfile)
        reader = csv.reader(csvfile)
        borrowers = []
        for row in reader:
            borrowers.append({
                "borrower_id": int(row[0]),
                "name": row[1],
                "email": row[2]
            })
        if borrowers:
            borrowers_collection.insert_many(borrowers)

    # Insert Transactions
    with open("transactions.csv", "r") as csvfile:
        next(csvfile)
        reader = csv.reader(csvfile)
        transactions = []
        for row in reader:
            transactions.append({
                "transaction_id": int(row[0]),
                "book_id": int(row[1]),
                "borrower_id": int(row[2]),
                "borrow_date": row[3],
                "return_date": row[4]
            })
        if transactions:
            transactions_collection.insert_many(transactions)

    client.close()

if __name__ == "__main__":
    insert_data()
    print("Data inserted into MongoDB successfully!")
