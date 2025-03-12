from neo4j import GraphDatabase
import time
import statistics

# Set dataset size label (update accordingly for different experiments)
dataset_size = "1000k"  # Change to "250k", "500k", "750k", or "1000k"

# Mapping from dataset size to CSV file suffix
csv_mapping = {
    "250k": "25",
    "500k": "50",
    "750k": "75",
    "1000k": "100"
}

def load_data_from_csv(driver, dataset_size):
    """
    Clears the database and loads data from CSV files located in the Neo4j import directory.
    Assumes CSV files are named as:
      - books_<suffix>.csv
      - borrowers_<suffix>.csv
      - transactions_<suffix>.csv
    """
    suffix = csv_mapping.get(dataset_size, "25")
    print(f"Dataset size is: {dataset_size} and suffix is: {suffix}")
    books_file = f"file:///books_{suffix}.csv"
    borrowers_file = f"file:///borrowers_{suffix}.csv"
    transactions_file = f"file:///transactions_{suffix}.csv"
    
    with driver.session() as session:
        # Clear the entire database
        session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE n"))
        print("Database cleared.")

        # Load Borrowers
        session.execute_write(lambda tx: tx.run(f"""
            LOAD CSV WITH HEADERS FROM '{borrowers_file}' AS row
            CREATE (:Borrower {{
                borrower_id: toInteger(row.borrower_id),
                name: row.name,
                email: row.email
            }})
        """))
        print(f"Borrowers loaded from {borrowers_file}.")

        # Load Books
        session.execute_write(lambda tx: tx.run(f"""
            LOAD CSV WITH HEADERS FROM '{books_file}' AS row
            CREATE (:Book {{
                book_id: toInteger(row.book_id),
                title: row.title,
                author: row.author,
                year: toInteger(row.year),
                genre: row.genre
            }})
        """))
        print(f"Books loaded from {books_file}.")

        # Load Transactions
        session.execute_write(lambda tx: tx.run(f"""
            LOAD CSV WITH HEADERS FROM '{transactions_file}' AS row
            CREATE (:Transaction {{
                transaction_id: toInteger(row.transaction_id),
                book_id: toInteger(row.book_id),
                borrower_id: toInteger(row.borrower_id),
                borrow_date: row.borrow_date,
                return_date: row.return_date
            }})
        """))
        print(f"Transactions loaded from {transactions_file}.")

        # Create BORROWED relationships using data from Transaction nodes.
        session.execute_write(lambda tx: tx.run("""
            MATCH (br:Borrower), (b:Book), (t:Transaction)
            WHERE br.borrower_id = t.borrower_id AND b.book_id = t.book_id
            CREATE (br)-[:BORROWED {
                borrow_date: t.borrow_date,
                return_date: t.return_date
            }]->(b)
        """))
        print("BORROWED relationships created.")

def measure_neo4j_query(driver, query):
    times = []
    
    def run_query(tx):
        result = list(tx.run(query))
        return result

    with driver.session() as session:
        # Cold run: measure first execution time
        start = time.time()
        session.execute_read(run_query)
        first_time = (time.time() - start) * 1000  # in milliseconds

        # Run the query 30 additional times and record times
        for i in range(30):
            start = time.time()
            session.execute_read(run_query)
            elapsed = (time.time() - start) * 1000
            times.append(elapsed)
            print(f"Iteration {i+1} complete: {elapsed:.2f} ms")
               
        avg_time = sum(times) / len(times)
        conf_interval = 1.96 * statistics.stdev(times) / (len(times) ** 0.5)
        return first_time, avg_time, conf_interval

if __name__ == "__main__":
    # Prompt for a borrower name pattern for Query1 (e.g., "J" for names starting with J)
    name_pattern = input("Enter the borrower name pattern (e.g., 'J' for names starting with J): ")
    
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))
    
    print(f"Dataset Size: {dataset_size}")
    
    # Load data into Neo4j from CSV files corresponding to the dataset size.
    load_data_from_csv(driver, dataset_size)
    
    # Define four Cypher queries with increasing complexity.
    # Query1 is modified to use the input name pattern.
    queries = {
        "Query1": f"""
           MATCH (br:Borrower)
           WHERE br.name =~ '(?i)^{name_pattern}.*'
           RETURN br.name AS name
       """,
        "Query2": """
           MATCH (br:Borrower)-[:BORROWED]->(b:Book)
           WHERE b.genre = 'Fiction'
           RETURN br.name AS name, count(b) AS borrow_count
       """,
        "Query3": """
           MATCH (br:Borrower)-[:BORROWED]->(b:Book)
           RETURN b.title AS title, count(*) AS borrow_count
           ORDER BY borrow_count DESC
           LIMIT 5
       """,
        "Query4": """
           MATCH (br:Borrower)-[r:BORROWED]->(b:Book)
           WHERE r.borrow_date >= '2022-01-01'
           WITH br, count(r) AS borrowCount
           WHERE borrowCount > 2
           MATCH (br)-[r:BORROWED]->(b:Book)
           RETURN br.name AS name, b.title AS title, r.borrow_date AS borrow_date, r.return_date AS return_date
       """
    }
    
    # Run performance tests for each query.
    for query_name, query in queries.items():
        print(f"\nRunning {query_name}...")
        first_time, avg_time, conf_interval = measure_neo4j_query(driver, query)
        print(f"{query_name} Performance:")
        print(f"  First Execution Time: {first_time:.2f} ms")
        print(f"  Average Execution Time: {avg_time:.2f} ms")
        print(f"  95% Confidence Interval: ±{conf_interval:.2f} ms")
    
    # After all queries have run, run Query1 again to print the borrower names.
    with driver.session() as session:
        result = list(session.execute_read(lambda tx: list(tx.run(queries["Query1"]))))
        names = sorted([record["name"] for record in result])
        print(f"\nBorrowers whose names start with '{name_pattern}':")
        for name in names:
            print(name)
    
    driver.close()
