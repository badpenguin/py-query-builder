from QueryBuilder import Database


def test_query_builder():
    print('- Database ')
    db = Database("sqlite://:memory:")

    # Create a test table
    print('- Create table')
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

    # Insert
    sql = db.table('test').insert({"id": 2, "name": "Alice"})
    print('- Insert: ', sql.build_query(), sql.debug_params())
    sql.run()
    print('- Affected rows: ', db.row_count())
    print('- Last insert ID: ', db.last_insert_id())

    # Insert (DUPLICATE)
    #sql = db.table('test').insert({"id": 2, "name": "Alice"}, True)
    #print('- Duplicate (MySQL only): ', sql.build_query(), sql.debug_params())

    # Update
    sql = db.table('test').update({"name": "Antonio"}).where('id = :id', {'id': 2})
    print('- Update: ', sql.build_query(), sql.debug_params())
    sql.run()
    print('- Affected rows: ', db.row_count())

    # Update FAILED
    sql = db.table('test').update({"name": "Antonio"}).where('id = :id', {'id': 1}).where('id = :id2', {'id2': 2})
    print('- Update: ', sql.build_query(), sql.debug_params())
    sql.run()
    print('- Affected rows: ', db.row_count())

    # SELECT FULL
    sql = db.table('test')
    print('- Select: ', sql.build_query(), sql.debug_params())

    # SELECT * + order by
    sql = db.table('test').select().order_by('name')
    print('- Select: ', sql.build_query(), sql.debug_params())

    # SELECT group by + having
    sql = db.table('test').select('count(*) as counter, name').order_by('counter desc').group_by('name') \
        .having('counter > 0').limit(10).offset('0')
    print('- Having: ', sql.build_query(), sql.debug_params())
    data = sql.fetch_row()
    print('- row: ', data)

    # SELECT group by + having 2
    sql = db.table('test').select('count(*) as counter, name').group_by('name').having('counter > :v1', {'v1': 0})
    print('- Having: ', sql.build_query(), sql.debug_params())
    data = sql.fetch_column()
    print('- column: ', data)

    # Prepare for join
    db.execute("CREATE TABLE test2 (id2 INTEGER PRIMARY KEY, name2 TEXT)")
    db.table('test2').insert({"id2": 2, "name2": "Wonderland"}).run()

    # Inner Join
    sql = db.table('test t1').join('test2 t2', 't1.id=t2.id2')
    print('- Join: ', sql.build_query(), sql.debug_params())
    data = sql.fetch_all()
    print('- all: ', data)

    # Left Join with Params
    sql = db.table('test t1').left_join('test2 t2', 't1.id=t2.id2 and t1.id=:pk', {'pk': '2'})
    print('- Join: ', sql.build_query(), sql.debug_params())
    data = sql.fetch_all()
    print('- all: ', data)

    # RIGHT + FULL JOIN are not supported by memory

    # Done
    db.close()


test_query_builder()
