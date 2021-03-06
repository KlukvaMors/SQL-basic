CREATE TABLE book (
    book_id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(50) NOT NULL,
    author VARCHAR(30) NOT NULL,
    price DECIMAL(8, 3) NOT NULL,
    amount INTEGER NOT NULL,
    
    UNIQUE(title, author) ON CONFLICT IGNORE
)