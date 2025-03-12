from cassandra.cluster import Cluster
import csv

def insert_data():
    # Connect to the Cassandra cluster on localhost
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('library')

    # Insert Books
    with open('books.csv', 'r') as csvfile:
        next(csvfile)  # Skip header
        reader = csv.reader(csvfile)
        for row in reader:
            query = """
                INSERT INTO books (book_id, title, author, year, genre)
                VALUES (%s, %s, %s, %s, %s)
            """
            session.execute(query, (int(row[0]), row[1], row[2], int(row[3]), row[4]))

    # Insert Borrowers
    with open('borrowers.csv', 'r') as csvfile:
        next(csvfile)
        reader = csv.reader(csvfile)
        for row in reader:
            query = """
                INSERT INTO borrowers (borrower_id, name, email)
                VALUES (%s, %s, %s)
            """
            session.execute(query, (int(row[0]), row[1], row[2]))

    # Insert Transactions
    with open('transactions.csv', 'r') as csvfile:
        next(csvfile)
        reader = csv.reader(csvfile)
        for row in reader:
            query = """
                INSERT INTO transactions (transaction_id, book_id, borrower_id, borrow_date, return_date)
                VALUES (%s, %s, %s, %s, %s)
            """
            session.execute(query, (int(row[0]), int(row[1]), int(row[2]), row[3], row[4]))

    cluster.shutdown()

if __name__ == "__main__":
    insert_data()
    print("Data inserted into Cassandra successfully!")
