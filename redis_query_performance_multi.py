import time
import statistics
import redis
import csv

# Set dataset size label (e.g., "250k", "500k", "750k", or "1000k")
dataset_size = "250k"  # Change as needed

# Mapping from dataset size to CSV file suffix
csv_mapping = {
    "250k": "25",
    "500k": "50",
    "750k": "75",
    "1000k": "100"
}

def load_data_from_csv(dataset_size, r):
    """
    Clears the Redis database and loads data from CSV subset files.
    Expected CSV files:
      - books_<suffix>.csv
      - borrowers_<suffix>.csv
      - transactions_<suffix>.csv
    """
    suffix = csv_mapping.get(dataset_size, "25")
    print(f"Dataset size is: {dataset_size} and suffix is: {suffix}")
    # Clear the database
    r.flushdb()
    print("Redis database cleared.")
    
    books_file = f"books_{suffix}.csv"
    borrowers_file = f"borrowers_{suffix}.csv"
    transactions_file = f"transactions_{suffix}.csv"
    
    # Load Books
    with open(books_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = f"book:{row['book_id']}"
            r.hset(key, mapping=row)
    print(f"Books loaded from {books_file}.")
    
    # Load Borrowers
    with open(borrowers_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = f"borrower:{row['borrower_id']}"
            r.hset(key, mapping=row)
    print(f"Borrowers loaded from {borrowers_file}.")
    
    # Load Transactions
    with open(transactions_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = f"transaction:{row['transaction_id']}"
            r.hset(key, mapping=row)
    print(f"Transactions loaded from {transactions_file}.")

# Prompt the user for a borrower name pattern for Query1 (e.g., "J" for names starting with J)
name_pattern = input("Enter the borrower name pattern (e.g., 'J' for names starting with J): ")

# Define Query Functions for Redis.
# Query1: Retrieve borrower names whose names start with the given pattern.
def redis_query1():
    matching = []
    for key in r.scan_iter("borrower:*"):
        borrower = r.hgetall(key)
        name = borrower.get(b'name', b'').decode()
        if name.lower().startswith(name_pattern.lower()):
            matching.append(name)
    return sorted(matching)

# Query2: Retrieve list of borrowers with count of Fiction books they've borrowed.
def redis_query2():
    book_genre_cache = {}
    def get_book_genre(book_id):
        if book_id in book_genre_cache:
            return book_genre_cache[book_id]
        book = r.hgetall(f"book:{book_id}")
        genre = book.get(b'genre', b'').decode() if book else None
        book_genre_cache[book_id] = genre
        return genre

    fiction_counts = {}
    for tkey in r.scan_iter("transaction:*"):
        trans = r.hgetall(tkey)
        borrower_id = trans.get(b'borrower_id', b'').decode()
        book_id = trans.get(b'book_id', b'').decode()
        if get_book_genre(book_id) == "Fiction":
            fiction_counts[borrower_id] = fiction_counts.get(borrower_id, 0) + 1

    results = []
    for bkey in r.scan_iter("borrower:*"):
        borrower = r.hgetall(bkey)
        borrower_id = bkey.decode().split(":")[1]
        name = borrower.get(b'name', b'').decode()
        count = fiction_counts.get(borrower_id, 0)
        results.append((name, count))
    return results

# Query3: Retrieve top 5 most popular books based on borrowing frequency.
def redis_query3():
    freq = {}
    for tkey in r.scan_iter("transaction:*"):
        trans = r.hgetall(tkey)
        book_id = trans.get(b'book_id', b'').decode()
        freq[book_id] = freq.get(book_id, 0) + 1
    top5 = sorted(freq.items(), key=lambda x: x[1], reverse=True)[:5]
    results = []
    for book_id, count in top5:
        book = r.hgetall(f"book:{book_id}")
        title = book.get(b'title', b'').decode() if book else "Unknown"
        results.append((title, count))
    return results

# Query4: Retrieve detailed borrowing history for borrowers who have borrowed more than 2 books since '2022-01-01'.
def redis_query4():
    borrower_history = {}
    for tkey in r.scan_iter("transaction:*"):
        trans = r.hgetall(tkey)
        borrow_date = trans.get(b'borrow_date', b'').decode()
        if borrow_date >= "2022-01-01":
            borrower_id = trans.get(b'borrower_id', b'').decode()
            borrower_history.setdefault(borrower_id, []).append(tkey.decode())
    results = []
    for borrower_id, tkeys in borrower_history.items():
        if len(tkeys) > 2:
            borrower = r.hgetall(f"borrower:{borrower_id}")
            name = borrower.get(b'name', b'').decode() if borrower else "Unknown"
            for tkey in tkeys:
                trans = r.hgetall(tkey)
                book_id = trans.get(b'book_id', b'').decode()
                book = r.hgetall(f"book:{book_id}")
                title = book.get(b'title', b'').decode() if book else "Unknown"
                borrow_date = trans.get(b'borrow_date', b'').decode()
                return_date = trans.get(b'return_date', b'').decode()
                results.append((name, title, borrow_date, return_date))
    return results

# Mapping of query names to their functions.
queries = {
    "Query1": redis_query1,
    "Query2": redis_query2,
    "Query3": redis_query3,
    "Query4": redis_query4
}

def measure_redis_query(query_func):
    times = []
    # Cold run.
    start = time.time()
    query_func()
    first_time = (time.time() - start) * 1000  # in ms
    # 30 subsequent runs.
    for i in range(30):
        start = time.time()
        query_func()
        elapsed = (time.time() - start) * 1000
        times.append(elapsed)
        print(f"Iteration {i+1} complete: {elapsed:.2f} ms")
    avg_time = sum(times) / len(times)
    conf_interval = 1.96 * statistics.stdev(times) / (len(times) ** 0.5)
    return first_time, avg_time, conf_interval

if __name__ == "__main__":
    print(f"Dataset Size: {dataset_size}")
    
    # Connect to Redis.
    r = redis.Redis(host="localhost", port=6379, db=0)
    
    # Load data from the appropriate CSV subset into Redis.
    load_data_from_csv(dataset_size, r)
    
    # Run performance tests for each query.
    for query_name, func in queries.items():
        print(f"\nRunning {query_name}...")
        first_time, avg_time, conf_interval = measure_redis_query(func)
        print(f"{query_name} Performance:")
        print(f"  First Execution Time: {first_time:.2f} ms")
        print(f"  Average Execution Time: {avg_time:.2f} ms")
        print(f"  95% Confidence Interval: ±{conf_interval:.2f} ms")
    
    # After all queries have been executed, run Query1 again to print the matching borrower names.
    results = redis_query1()
    print(f"\nBorrowers whose names start with '{name_pattern}':")
    for name in results:
        print(name)
