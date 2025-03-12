import csv
import redis

def insert_data():
    # Connect to Redis running on localhost
    r = redis.Redis(host='localhost', port=6379, db=0)

    # Insert Books
    with open('books.csv', 'r') as csvfile:
        next(csvfile)  # Skip header
        reader = csv.reader(csvfile)
        for row in reader:
            key = f"book:{row[0]}"
            r.hset(key, mapping={
                'title': row[1],
                'author': row[2],
                'year': row[3],
                'genre': row[4]
            })

    # Insert Borrowers
    with open('borrowers.csv', 'r') as csvfile:
        next(csvfile)
        reader = csv.reader(csvfile)
        for row in reader:
            key = f"borrower:{row[0]}"
            r.hset(key, mapping={
                'name': row[1],
                'email': row[2]
            })

    # Insert Transactions
    with open('transactions.csv', 'r') as csvfile:
        next(csvfile)
        reader = csv.reader(csvfile)
        for row in reader:
            key = f"transaction:{row[0]}"
            r.hset(key, mapping={
                'book_id': row[1],
                'borrower_id': row[2],
                'borrow_date': row[3],
                'return_date': row[4]
            })

if __name__ == "__main__":
    insert_data()
    print("Data inserted into Redis successfully!")
