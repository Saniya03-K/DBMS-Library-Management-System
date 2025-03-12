import csv
from faker import Faker
import random

fake = Faker()

def generate_books(n, filename):
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['book_id', 'title', 'author', 'year', 'genre']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(n):
            writer.writerow({
                'book_id': i + 1,
                'title': fake.sentence(nb_words=4),
                'author': fake.name(),
                'year': random.randint(1900, 2023),
                'genre': random.choice(['Fiction', 'Non-Fiction', 'Science', 'History', 'Biography'])
            })

def generate_borrowers(n, filename):
    emails = set()
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['borrower_id', 'name', 'email']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(n):
            name = fake.name()
            email = fake.email()
            #Ensure email uniqueness
            while email in emails:
               email = fake.email()
            emails.add(email)
            writer.writerow({
                'borrower_id': i + 1,
                'name': name,
                'email': email
            })

def generate_transactions(n, filename, num_books, num_borrowers):
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['transaction_id', 'book_id', 'borrower_id', 'borrow_date', 'return_date']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(n):
            borrow_date = fake.date_between(start_date="-2y", end_date="today")
            return_date = fake.date_between(start_date=borrow_date, end_date="today")
            writer.writerow({
                'transaction_id': i + 1,
                'book_id': random.randint(1, num_books),
                'borrower_id': random.randint(1, num_borrowers),
                'borrow_date': borrow_date,
                'return_date': return_date
            })

if __name__ == "__main__":
    # You can adjust these numbers to generate datasets of different sizes.
    num_books = 1000
    num_borrowers = 1000
    num_transactions = 2000  # Adjust based on your needs

    generate_books(num_books, 'books.csv')
    generate_borrowers(num_borrowers, 'borrowers.csv')
    generate_transactions(num_transactions, 'transactions.csv', num_books, num_borrowers)
