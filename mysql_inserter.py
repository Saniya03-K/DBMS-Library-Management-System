import csv
import mysql.connector

def clear_tables(cursor, conn):
    # Disable foreign key checks to allow deletion
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    cursor.execute("DELETE FROM transactions")
    cursor.execute("DELETE FROM borrowers")
    cursor.execute("DELETE FROM books")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()
    print("Tables cleared.")

def insert_books(cursor, conn):
    with open('books_25.csv', 'r') as csvfile:
        next(csvfile)  # Skip header
        reader = csv.reader(csvfile)
        for row in reader:
            cursor.execute(
                "INSERT INTO books (book_id, title, author, year, genre) VALUES (%s, %s, %s, %s, %s)", row
            )
    conn.commit()
    print("Books inserted successfully.")

def insert_borrowers(cursor, conn):
    with open('borrowers_25.csv', 'r') as csvfile:
        next(csvfile)  # Skip header
        reader = csv.reader(csvfile)
        for row in reader:
            cursor.execute(
                "INSERT INTO borrowers (borrower_id, name, email) VALUES (%s, %s, %s)", row
            )
    conn.commit()
    print("Borrowers inserted successfully.")

def insert_transactions(cursor, conn):
    # Disable foreign key checks
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    with open('transactions_25.csv', 'r') as csvfile:
        next(csvfile)  # Skip header
        reader = csv.reader(csvfile)
        for row in reader:
            cursor.execute(
                "INSERT INTO transactions (transaction_id, book_id, borrower_id, borrow_date, return_date) VALUES (%s, %s, %s, %s, %s)", row
            )
    conn.commit()
    # Re-enable foreign key checks
    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
    print("Transactions inserted successfully.")

def main():
    # Database connection
    conn = mysql.connector.connect(
        host="127.0.0.1",
        port=3307,
        user="user",
        password="userpassword",
        database="library"
    )
    cursor = conn.cursor()

    clear_tables(cursor, conn)
    insert_books(cursor, conn)
    insert_borrowers(cursor, conn)
    insert_transactions(cursor, conn)

    cursor.close()
    conn.close()
    print("25% subset data inserted into MySQL successfully!")

if __name__ == "__main__":
    main()
