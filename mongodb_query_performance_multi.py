import time
import statistics
import csv
from pymongo import MongoClient

# Set dataset size label (e.g., "250k", "500k", "750k", or "1000k")
dataset_size = "250k"  # Change as needed

# Mapping from dataset size to CSV file suffix
csv_mapping = {
    "250k": "25",
    "500k": "50",
    "750k": "75",
    "1000k": "100"
}

def load_data_from_csv(dataset_size, db):
    """
    Clears the MongoDB collections and loads data from the CSV subset files
    based on the given dataset size.
    Expected CSV files: books_<suffix>.csv, borrowers_<suffix>.csv, transactions_<suffix>.csv
    """
    suffix = csv_mapping.get(dataset_size, "25")
    print(f"Dataset size is: {dataset_size} and suffix is: {suffix}")
    books_file = f"books_{suffix}.csv"
    borrowers_file = f"borrowers_{suffix}.csv"
    transactions_file = f"transactions_{suffix}.csv"
    
    # Clear existing data
    db.books.delete_many({})
    db.borrowers.delete_many({})
    db.transactions.delete_many({})
    print("Collections cleared.")
    
    # Insert Books
    with open(books_file, "r") as f:
        reader = csv.DictReader(f)
        books_list = []
        for row in reader:
            row["book_id"] = int(row["book_id"])
            row["year"] = int(row["year"])
            books_list.append(row)
        if books_list:
            db.books.insert_many(books_list)
    print(f"Books inserted successfully from {books_file}.")
    
    # Insert Borrowers
    with open(borrowers_file, "r") as f:
        reader = csv.DictReader(f)
        borrowers_list = []
        for row in reader:
            row["borrower_id"] = int(row["borrower_id"])
            borrowers_list.append(row)
        if borrowers_list:
            db.borrowers.insert_many(borrowers_list)
    print(f"Borrowers inserted successfully from {borrowers_file}.")
    
    # Insert Transactions
    with open(transactions_file, "r") as f:
        reader = csv.DictReader(f)
        transactions_list = []
        for row in reader:
            row["transaction_id"] = int(row["transaction_id"])
            row["book_id"] = int(row["book_id"])
            row["borrower_id"] = int(row["borrower_id"])
            transactions_list.append(row)
        if transactions_list:
            db.transactions.insert_many(transactions_list)
    print(f"Transactions inserted successfully from {transactions_file}.")

def measure_pipeline(pipeline, collection):
    times = []
    # Cold run
    start = time.time()
    list(collection.aggregate(pipeline))
    first_time = (time.time() - start) * 1000  # in ms
    # 30 subsequent runs
    for _ in range(30):
        start = time.time()
        list(collection.aggregate(pipeline))
        times.append((time.time() - start) * 1000)
    avg_time = sum(times) / len(times)
    conf_interval = 1.96 * statistics.stdev(times) / (len(times) ** 0.5)
    return first_time, avg_time, conf_interval

if __name__ == "__main__":
    # Prompt the user for a borrower name pattern for Query1 (e.g., "S" for names starting with S)
    name_pattern = input("Enter the borrower name pattern (e.g., 'S' for names starting with S): ")
    
    client = MongoClient("mongodb://localhost:27017/")
    db = client["library"]

    print(f"Dataset Size: {dataset_size}")
    
    # Load the appropriate data into MongoDB based on dataset_size.
    load_data_from_csv(dataset_size, db)
    
    # Define four aggregation pipelines corresponding to four queries.
    # Query1: Return borrowers whose names start with the input pattern.
    queries = {
        "Query1": [
            {"$match": {"name": {"$regex": f"^{name_pattern}", "$options": "i"}}},
            {"$project": {"_id": 0, "name": 1}}
        ],
        "Query2": [
            {"$lookup": {
                "from": "transactions",
                "localField": "borrower_id",
                "foreignField": "borrower_id",
                "as": "transactions"
            }},
            {"$unwind": "$transactions"},
            {"$lookup": {
                "from": "books",
                "localField": "transactions.book_id",
                "foreignField": "book_id",
                "as": "book"
            }},
            {"$unwind": "$book"},
            {"$match": {"book.genre": "Fiction"}},
            {"$group": {"_id": "$borrower_id", "name": {"$first": "$name"}, "borrow_count": {"$sum": 1}}}
        ],
        "Query3": [
            {"$group": {"_id": "$book_id", "borrow_count": {"$sum": 1}}},
            {"$sort": {"borrow_count": -1}},
            {"$limit": 5},
            {"$lookup": {
                "from": "books",
                "localField": "_id",
                "foreignField": "book_id",
                "as": "book"
            }},
            {"$unwind": "$book"},
            {"$project": {"title": "$book.title", "borrow_count": 1, "_id": 0}}
        ],
        "Query4": [
            {"$match": {"borrow_date": {"$gte": "2022-01-01"}}},  # Adjust this date if needed
            {"$group": {"_id": "$borrower_id", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 2}}},
            {"$lookup": {
                "from": "borrowers",
                "localField": "_id",
                "foreignField": "borrower_id",
                "as": "borrower"
            }},
            {"$unwind": "$borrower"},
            {"$lookup": {
                "from": "transactions",
                "localField": "_id",
                "foreignField": "borrower_id",
                "as": "transactions"
            }},
            {"$unwind": "$transactions"},
            {"$lookup": {
                "from": "books",
                "localField": "transactions.book_id",
                "foreignField": "book_id",
                "as": "book"
            }},
            {"$unwind": "$book"},
            {"$project": {"name": "$borrower.name", "title": "$book.title", "borrow_date": "$transactions.borrow_date", "return_date": "$transactions.return_date", "_id": 0}}
        ]
    }
    
    # Run performance tests for each query.
    for query_name, pipeline in queries.items():
        # For Query1 and Query2, run on the "borrowers" collection;
        # For Query3 and Query4, run on the "transactions" collection.
        if query_name in ["Query1", "Query2"]:
            collection = db["borrowers"]
        else:
            collection = db["transactions"]

        first_time, avg_time, conf_interval = measure_pipeline(pipeline, collection)
        print(f"\n{query_name} Performance:")
        print(f"  First Execution Time: {first_time:.2f} ms")
        print(f"  Average Execution Time: {avg_time:.2f} ms")
        print(f"  95% Confidence Interval: ±{conf_interval:.2f} ms")
    
    # After running all queries, print the sorted list of borrowers for Query1.
    # Run Query1 on the borrowers collection.
    query1_results = list(db.borrowers.aggregate(queries["Query1"]))
    if query1_results:
        names = sorted([res["name"] for res in query1_results])
        print(f"\nBorrowers whose names start with '{name_pattern}':")
        for name in names:
            print(name)
    else:
        print(f"\nNo borrowers found with names starting with '{name_pattern}'.")
    
    client.close()
