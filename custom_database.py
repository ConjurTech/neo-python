import psycopg2
import json
import copy
import binascii
import os


class CustomDatabase(object):

    def write_event_to_psql(self, event):
        print(event)
        print('WRITING TO PSQL')

        # Prepare variables
        event_type = event.event_type.encode('utf-8')
        tmp_payload = copy.deepcopy(event.event_payload)
        tmp_payload.pop(0)
        event_payload = list(map(lambda x: self.parse_bytes(x), tmp_payload))
        contract_hash = event.contract_hash
        block_number = event.block_number
        tx_hash = event.tx_hash
        execution_success = event.execution_success
        test_mode = event.test_mode

        try:
            # conn = psycopg2.connect("dbname="+ neonode + " user=" + myuser + " password=" + mypass + " host=" + localhost")
            conn = psycopg2.connect("dbname=" + os.environ['NEO_PYTHON_DBNAME'] +
                                    " user=" + os.environ['NEO_PYTHON_USER'] +
                                    " password=" + os.environ['NEO_PYTHON_PASSWORD'] +
                                    " host=" + os.environ['NEO_PYTHON_HOST'])
        except Exception as ex:
            print('failed to connect')
            print("Error: " + str(ex))

        print(conn)
        cur = conn.cursor()
        # cur.execute("CREATE TABLE events (id serial PRIMARY KEY, block_number varchar, transaction_hash varchar, contract_hash varchar, event_type varchar, data jsonb);")
        data = {'event_payload': event_payload,
                'execution_success': execution_success,
                'test_mode': test_mode}
        print('DATA')
        print(data)
        print(str(block_number))
        print(str(tx_hash))
        print(str(contract_hash))
        cur.execute("INSERT INTO events (block_number, transaction_hash, contract_hash, event_type, data) VALUES (%s, %s, %s, %s, %s)",
                    (str(block_number), str(tx_hash), str(contract_hash), event_type, json.dumps(data)))
        cur.execute("SELECT * FROM events;")
        print(cur.fetchall())
        conn.commit()
        cur.close()
        conn.close()

    def parse_bytes(self, bytes):
        if len(str(bytes)) <= 4:
            print('int')
            print(bytes)
            return int.from_bytes(bytes, byteorder='little')
        elif len(str(bytes)) >= 40:
            print('hex')
            print(bytes)
            hex = binascii.hexlify(bytes)
            ba = bytearray(hex)
            ba.reverse()
            return str(ba, 'utf-8')
        else:
            return bytes





