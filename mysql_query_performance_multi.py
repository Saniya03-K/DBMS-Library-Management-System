import time
import statistics
import mysql.connector
import csv

# Set this variable manually before each run based on the loaded dataset:
# For example, set to "250k" if you want to load the 25% subset (which represents 250k records)
dataset_size = "1000k"  # Change to "250k", "500k", "750k", or "1000k" as needed

# Mapping from dataset_size to CSV file suffix
csv_mapping = {
    "250k": "25",
    "500k": "50",
    "750k": "75",
    "1000k": "100"
}

def load_data_from_csv(dataset_size):
    """
    Clears the MySQL tables and loads data from the CSV subset files corresponding to the given dataset size.
    """
    suffix = csv_mapping.get(dataset_size, "25")
    print(f"Dataset size is: {dataset_size} and suffix is: {suffix}")
    books_file = f"books_{suffix}.csv"
    borrowers_file = f"borrowers_{suffix}.csv"
    transactions_file = f"transactions_{suffix}.csv"
    
    # Connect to MySQL
    conn = mysql.connector.connect(
        host="127.0.0.1",
        port=3307,
        user="user",
        password="userpassword",
        database="library"
    )
    cursor = conn.cursor()
    
    # Clear existing data
    cursor.execute("DELETE FROM transactions;")
    cursor.execute("DELETE FROM borrowers;")
    cursor.execute("DELETE FROM books;")
    conn.commit()
    print("Tables cleared.")
    
    # Insert Books
    with open(books_file, 'r') as f:
        next(f)  # Skip header
        reader = csv.reader(f)
        for row in reader:
            cursor.execute(
                "INSERT INTO books (book_id, title, author, year, genre) VALUES (%s, %s, %s, %s, %s)", row
            )
    conn.commit()
    print(f"Books inserted successfully from {books_file}.")
    
    # Insert Borrowers
    with open(borrowers_file, 'r') as f:
        next(f)
        reader = csv.reader(f)
        for row in reader:
            cursor.execute(
                "INSERT INTO borrowers (borrower_id, name, email) VALUES (%s, %s, %s)", row
            )
    conn.commit()
    print(f"Borrowers inserted successfully from {borrowers_file}.")
    
    # Insert Transactions with foreign key checks disabled
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    with open(transactions_file, 'r') as f:
        next(f)
        reader = csv.reader(f)
        for row in reader:
            cursor.execute(
                "INSERT INTO transactions (transaction_id, book_id, borrower_id, borrow_date, return_date) VALUES (%s, %s, %s, %s, %s)", row
            )
    conn.commit()
    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
    print(f"Transactions inserted successfully from {transactions_file}.")
    
    cursor.close()
    conn.close()

# Prompt the user for a name pattern to dynamically update Query1
name_pattern = input("Enter the borrower name pattern (e.g., 'S' for names starting with S): ")

# Define the four queries with increasing complexity
query1 = f"""
    SELECT DISTINCT br.name
    FROM books b 
    JOIN transactions t ON b.book_id = t.book_id 
    JOIN borrowers br ON t.borrower_id = br.borrower_id 
    WHERE br.name LIKE '{name_pattern}%';
"""

query2 = """
    SELECT br.name, COUNT(*) AS borrow_count 
    FROM borrowers br 
    JOIN transactions t ON br.borrower_id = t.borrower_id 
    JOIN books b ON t.book_id = b.book_id 
    WHERE b.genre = 'Fiction' 
    GROUP BY br.borrower_id, br.name;
"""

query3 = """
    SELECT b.title, COUNT(*) AS borrow_count 
    FROM books b 
    JOIN transactions t ON b.book_id = t.book_id 
    GROUP BY b.book_id, b.title 
    ORDER BY borrow_count DESC 
    LIMIT 5;
"""

query4 = """
    SELECT br.name, b.title, t.borrow_date, t.return_date 
    FROM borrowers br 
    JOIN transactions t ON br.borrower_id = t.borrower_id 
    JOIN books b ON t.book_id = b.book_id 
    WHERE br.borrower_id IN (
        SELECT borrower_id 
        FROM transactions 
        WHERE borrow_date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR) 
        GROUP BY borrower_id 
        HAVING COUNT(*) > 2
    );
"""

queries = {
    "Query1": query1,
    "Query2": query2,
    "Query3": query3,
    "Query4": query4
}

def fetch_borrowers(query):
    # Connect to the MySQL database
    conn = mysql.connector.connect(
        host="127.0.0.1",
        port=3307,
        user="user",
        password="userpassword",
        database="library"
    )
    cursor = conn.cursor()
    
    # Execute the query
    cursor.execute(query)
    
    # Fetch all matching results
    results = cursor.fetchall()

    # Print results in a cleaner format
    if results:
        print(f"Borrowers whose names start with '{name_pattern}':")
        for row in results:
            if len(row) == 1:
                print(row[0])
            else:
                print(row)
    else:
        print(f"No borrowers found with names starting with '{name_pattern}'.")
    
    cursor.close()
    conn.close()

def measure_query(query):
    # Connect to your MySQL database (ensure it contains the current subset)
    conn = mysql.connector.connect(
        host="127.0.0.1",
        port=3307,
        user="user",
        password="userpassword",
        database="library"
    )
    cursor = conn.cursor()
    times = []

    # Cold run: measure the first execution time
    start = time.time()
    cursor.execute(query)
    cursor.fetchall()
    first_time = (time.time() - start) * 1000  # in ms

    # Run the query 30 additional times and record each execution time
    for _ in range(30):
        start = time.time()
        cursor.execute(query)
        cursor.fetchall()
        times.append((time.time() - start) * 1000)
    avg_time = sum(times) / len(times)
    conf_interval = 1.96 * statistics.stdev(times) / (len(times) ** 0.5)

    cursor.close()
    conn.close()
    return first_time, avg_time, conf_interval

if __name__ == "__main__":
    print(f"Dataset Size: {dataset_size}")
    
    # Load the appropriate data from CSV files based on dataset_size.
    load_data_from_csv(dataset_size)
    
    # Now run performance tests for each query
    for query_name, query in queries.items():
        first_time, avg_time, conf_interval = measure_query(query)
        print(f"{query_name} Performance:")
        print(f"  First Execution Time: {first_time:.2f} ms")
        print(f"  Average Execution Time: {avg_time:.2f} ms")
        print(f"  95% Confidence Interval: ±{conf_interval:.2f} ms\n")
    
    # Fetch and display borrowers based on the name pattern using the modified Query1
    fetch_borrowers(query1)
