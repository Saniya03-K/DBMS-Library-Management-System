from cassandra.cluster import Cluster
import time
import statistics
import csv

# Set this variable manually based on the desired dataset size:
# "250k" for 25%, "500k" for 50%, "750k" for 75%, "1000k" for 100%
dataset_size = "250k"  # Change as needed

# Mapping from dataset size to CSV file suffix
csv_mapping = {
    "250k": "25",
    "500k": "50",
    "750k": "75",
    "1000k": "100"
}

def load_data_from_csv(dataset_size, session):
    """
    Clears Cassandra tables and loads data from CSV subset files based on the given dataset size.
    Expected CSV files: books_<suffix>.csv, borrowers_<suffix>.csv, transactions_<suffix>.csv
    """
    suffix = csv_mapping.get(dataset_size, "25")
    books_file = f"books_{suffix}.csv"
    borrowers_file = f"borrowers_{suffix}.csv"
    transactions_file = f"transactions_{suffix}.csv"
    
    # Clear existing data using TRUNCATE
    session.execute("TRUNCATE books;")
    session.execute("TRUNCATE borrowers;")
    session.execute("TRUNCATE transactions;")
    print("Tables truncated.")
    
    # Load Books
    with open(books_file, 'r') as f:
        next(f)  # skip header
        reader = csv.reader(f)
        for row in reader:
            session.execute(
                """
                INSERT INTO books (book_id, title, author, year, genre)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (int(row[0]), row[1], row[2], int(row[3]), row[4])
            )
    print(f"Books inserted successfully from {books_file}.")
    
    # Load Borrowers
    with open(borrowers_file, 'r') as f:
        next(f)
        reader = csv.reader(f)
        for row in reader:
            session.execute(
                """
                INSERT INTO borrowers (borrower_id, name, email)
                VALUES (%s, %s, %s)
                """,
                (int(row[0]), row[1], row[2])
            )
    print(f"Borrowers inserted successfully from {borrowers_file}.")
    
    # Load Transactions
    with open(transactions_file, 'r') as f:
        next(f)
        reader = csv.reader(f)
        for row in reader:
            session.execute(
                """
                INSERT INTO transactions (transaction_id, book_id, borrower_id, borrow_date, return_date)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (int(row[0]), int(row[1]), int(row[2]), row[3], row[4])
            )
    print(f"Transactions inserted successfully from {transactions_file}.")

# --- Query Functions for Cassandra ---

# Modified Query1: Return all borrowers whose names start with a given pattern.
def query1(session, name_pattern):
    print("Running Query1...")
    # Fetch all borrowers (using ALLOW FILTERING for simplicity)
    rows = session.execute("SELECT borrower_id, name FROM borrowers ALLOW FILTERING;")
    # Filter in Python for names starting with the pattern (case-insensitive)
    matching = [row.name for row in rows if row.name.lower().startswith(name_pattern.lower())]
    matching_sorted = sorted(matching)
    print(f"Query1 completed: found {len(matching_sorted)} matching borrowers.")
    return matching_sorted

def query2(session):
    print("Running Query2...")
    results = []
    rows = session.execute("SELECT borrower_id, name FROM borrowers ALLOW FILTERING;")
    for row in rows:
        borrower_id = row.borrower_id
        name = row.name
        rows2 = session.execute(f"SELECT book_id FROM transactions WHERE borrower_id = {borrower_id} ALLOW FILTERING;")
        count = 0
        for r in rows2:
            row3 = session.execute(f"SELECT genre FROM books WHERE book_id = {r.book_id} ALLOW FILTERING;").one()
            if row3 and row3.genre == "Fiction":
                count += 1
        results.append((name, count))
    print(f"Query2 completed: processed {len(results)} borrowers.")
    return results

def query3(session):
    print("Running Query3...")
    rows = session.execute("SELECT book_id FROM transactions ALLOW FILTERING;")
    freq = {}
    for row in rows:
        freq[row.book_id] = freq.get(row.book_id, 0) + 1
    top5 = sorted(freq.items(), key=lambda x: x[1], reverse=True)[:5]
    top5_titles = []
    for book_id, count in top5:
        row = session.execute(f"SELECT title FROM books WHERE book_id = {book_id} ALLOW FILTERING;").one()
        if row:
            top5_titles.append((row.title, count))
    print("Query3 completed: top 5 titles retrieved.")
    return top5_titles

def query4(session):
    print("Running Query4...")
    rows = session.execute("SELECT borrower_id, borrow_date FROM transactions WHERE borrow_date >= '2022-01-01' ALLOW FILTERING;")
    borrower_counts = {}
    for row in rows:
        borrower_counts[row.borrower_id] = borrower_counts.get(row.borrower_id, 0) + 1
    eligible_borrowers = [bid for bid, count in borrower_counts.items() if count > 2]
    detailed_history = []
    for borrower_id in eligible_borrowers:
        row = session.execute(f"SELECT name FROM borrowers WHERE borrower_id = {borrower_id} ALLOW FILTERING;").one()
        name = row.name if row else "Unknown"
        rows2 = session.execute(f"SELECT book_id, borrow_date, return_date FROM transactions WHERE borrower_id = {borrower_id} ALLOW FILTERING;")
        for r in rows2:
            row3 = session.execute(f"SELECT title FROM books WHERE book_id = {r.book_id} ALLOW FILTERING;").one()
            title = row3.title if row3 else "Unknown"
            detailed_history.append((name, title, r.borrow_date, r.return_date))
    print(f"Query4 completed: found history for {len(eligible_borrowers)} borrowers.")
    return detailed_history

# --- Queries Dictionary ---
# For Query1, we use a lambda so that we can pass the name_pattern parameter.
queries = {
    "Query1": lambda session: query1(session, name_pattern),
    "Query2": query2,
    "Query3": query3,
    "Query4": query4
}

def measure_cassandra_query(query_func, session):
    times = []
    # Cold run
    start = time.time()
    query_func(session)
    first_time = (time.time() - start) * 1000  # ms
    # 30 subsequent runs
    for i in range(30):
        start = time.time()
        query_func(session)
        elapsed = (time.time() - start) * 1000
        times.append(elapsed)
        print(f"Iteration {i+1} complete: {elapsed:.2f} ms")
    avg_time = sum(times) / len(times)
    conf_interval = 1.96 * statistics.stdev(times) / (len(times) ** 0.5)
    return first_time, avg_time, conf_interval

if __name__ == "__main__":
    # Prompt the user for a borrower name pattern for Query1 (e.g., "J" or "Joshua")
    name_pattern = input("Enter the borrower name pattern (e.g., 'J' for names starting with J): ")
    
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('library')
    print(f"Dataset Size: {dataset_size}")
    
    # Load data from CSV subset into Cassandra.
    load_data_from_csv(dataset_size, session)
    
    # Measure performance for each query.
    for query_name, func in queries.items():
        print(f"\nMeasuring {query_name}...")
        first_time, avg_time, conf_interval = measure_cassandra_query(func, session)
        print(f"{query_name} Performance:")
        print(f"  First Execution Time: {first_time:.2f} ms")
        print(f"  Average Execution Time: {avg_time:.2f} ms")
        print(f"  95% Confidence Interval: ±{conf_interval:.2f} ms")
        
        # For Query1, print the sorted list of matching borrower names.
        if query_name == "Query1":
            results = func(session)  # Call query1 to get matching names
            print(f"Borrowers whose names start with '{name_pattern}':")
            for name in results:
                print(name)
    session.shutdown()
    cluster.shutdown()
