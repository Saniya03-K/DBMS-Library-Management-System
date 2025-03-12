from neo4j import GraphDatabase
import csv

# Configure the connection to your Neo4j container
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

def insert_data():
    with driver.session() as session:
        # Insert Book nodes
        with open("books.csv", "r") as csvfile:
            next(csvfile)  # Skip header
            reader = csv.reader(csvfile)
            for row in reader:
                session.run(
                    "CREATE (b:Book {book_id: $book_id, title: $title, author: $author, year: $year, genre: $genre})",
                    book_id=int(row[0]), title=row[1], author=row[2], year=int(row[3]), genre=row[4]
                )
        # Insert Borrower nodes
        with open("borrowers.csv", "r") as csvfile:
            next(csvfile)
            reader = csv.reader(csvfile)
            for row in reader:
                session.run(
                    "CREATE (br:Borrower {borrower_id: $borrower_id, name: $name, email: $email})",
                    borrower_id=int(row[0]), name=row[1], email=row[2]
                )
        # Create BORROWED relationships from transactions
        with open("transactions.csv", "r") as csvfile:
            next(csvfile)
            reader = csv.reader(csvfile)
            for row in reader:
                session.run(
                    """
                    MATCH (b:Book {book_id: $book_id}), (br:Borrower {borrower_id: $borrower_id})
                    CREATE (br)-[:BORROWED {transaction_id: $transaction_id, borrow_date: $borrow_date, return_date: $return_date}]->(b)
                    """,
                    transaction_id=int(row[0]),
                    book_id=int(row[1]),
                    borrower_id=int(row[2]),
                    borrow_date=row[3],
                    return_date=row[4]
                )

if __name__ == "__main__":
    insert_data()
    driver.close()
    print("Data inserted into Neo4j successfully!")
