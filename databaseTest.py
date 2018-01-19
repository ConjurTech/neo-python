import psycopg2
import json
try:
    conn = psycopg2.connect("dbname=neonode user=myuser password=mypass host=localhost")
except Exception as ex:
    print('failed to connect')
    print("Error: " + str(ex))

print(conn)
cur = conn.cursor()
# cur.execute("CREATE TABLE events (id serial PRIMARY KEY, block varchar, address varchar, data jsonb);")
block = "block1"
address = "address1"
data = {'data_key': 'data_value'}
# cur.execute("INSERT INTO events (block, address, data) VALUES (%s, %s, %s)",(block, address, json.dumps(data)))
cur.execute("SELECT * FROM events;")
print(cur.fetchall())
conn.commit()
cur.close()
conn.close()