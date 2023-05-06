# QueryBuilder

## What is it
QueryBuilder is a Python library for building SQL queries programmatically. It provides a simple and intuitive interface for creating and executing SQL queries, and supports common operations like SELECT, INSERT, UPDATE, and DELETE.

## Why
I am not a python developer, so I've asked chatgpt4 to develop a query builder.
After 3 days that the credit expired and I had to wait that it reset, after many frustration I dediced to take it over and finish it.

## Installation

To install QueryBuilder, simply run:

    pip install querybuilder

## Usage

Here's a simple example of using QueryBuilder to execute a SELECT query:

```python
from QueryBuilder import Database

db = Database("sqlite://:memory:")

db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
db.execute("CREATE TABLE test2 (id2 INTEGER PRIMARY KEY, name2 TEXT)")

db.table('test').insert({"id": 2, "name": "Alice"}).run()
db.table('test2').insert({"id2": 2, "name2": "Wonderland"}).run()

db.table('test').update({"name": "Antonio"}).where('id = :id', {'id': 2})

data = db.table('test').select('count(*) as counter, name').order_by('counter desc') \
        .group_by('name').having('counter > 0').limit(10).offset('0') \
        .fetch_row()

data = db.table('test t1').left_join('test2 t2', 't1.id=t2.id2') \
        .fetch_all()
```

For more information on how to use QueryBuilder, please see the test.py

# Contributing
If you'd like to contribute to QueryBuilder, please fork the repository and make a pull request. All contributions are welcome!

# License
QueryBuilder is licensed under the GPLv3 License. See LICENSE for more information.
