import psycopg2
import json
import copy

try:
    conn = psycopg2.connect("dbname=neonode user=myuser password=mypass host=localhost")
except Exception as ex:
    print('failed to connect')
    print("Error: " + str(ex))

class CustomDatabase(object):

    def write_event_to_psql(self, event):
        # SmartContractEvent(event_type=SmartContract.Runtime.Notify, event_payload=[b'transfer', b'\x00',
        #                                                                            b'\x02VyX\xee>N\xe6\xb1\x03D\x8e\n\xfe\x1f\xdf\x8bW\x96n\x1e\x87\x9cx;\x11e7\xec\xbfZ\xf9q',
        #                                                                            b'\x00\x80S\xee{\xa8\n'],
        #                    contract_hash=ecd24a4b2b31ee3144a71f7ac22dec6a3128190f, block_number=25231, tx_hash=03
        # 8
        # af3fefb63dabfd4e864e0a0ebe3d4b578116b8c0128b06de21a955392e838, execution_success = True, test_mode = False)
        print('WRITING TO PSQL')
        print(event)

        event_type = event.event_type
        tmp_payload = copy.deepcopy(event.event_payload)
        event_payload = list(map(lambda x: x.decode('utf-8', 'backslashreplace'), tmp_payload))
        contract_hash = event.contract_hash
        print(event.contract_hash)
        block_number = event.block_number
        tx_hash = event.tx_hash
        execution_success = event.execution_success
        test_mode = event.test_mode

        try:
            conn = psycopg2.connect("dbname=neonode user=myuser password=mypass host=localhost")
        except Exception as ex:
            print('failed to connect')
            print("Error: " + str(ex))

        print(conn)
        cur = conn.cursor()
        # cur.execute("CREATE TABLE events (id serial PRIMARY KEY, block varchar, address varchar, data jsonb);")
        block = str(block_number)
        address = str(tx_hash)
        data = {'event_type': event_type,
                'event_payload': event_payload,
                'contract_hash': str(contract_hash),
                'execution_success': execution_success,
                'test_mode': test_mode}
        print('DATA')
        print(data)
        cur.execute("INSERT INTO events (block, address, data) VALUES (%s, %s, %s)", (block, address, json.dumps(data)))
        cur.execute("SELECT * FROM events;")
        print(cur.fetchall())
        conn.commit()
        cur.close()
        conn.close()

