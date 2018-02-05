import plyvel
import psycopg2
import json
import os

from logzero import logger
from neo.EventHub import events
from neo.SmartContract.SmartContractEvent import SmartContractEvent, NotifyEvent, NotifyType
from neo.Settings import settings
from neo.Core.Blockchain import Blockchain


class NotificationPrefix():

    PREFIX_ADDR = b'\xCA'
#    PREFIX_CONTRACT = b'\xCB'
    PREFIX_BLOCK = b'\xCC'

    PREFIX_COUNT = b'\xCD'


class NotificationDB():

    __instance = None

    _events_to_write = None

    @staticmethod
    def instance():
        if not NotificationDB.__instance:
            if settings.NOTIFICATION_DB_PATH:
                NotificationDB.__instance = NotificationDB(settings.NOTIFICATION_DB_PATH)
#                logger.info("Created Notification DB At %s " % settings.NOTIFICATION_DB_PATH)
            else:
                logger.info("Notification DB Path not configured in settings")
        return NotificationDB.__instance

    @staticmethod
    def close():
        if NotificationDB.__instance:
            NotificationDB.__instance.db.close()
            NotificationDB.__instance = None

    @property
    def db(self):
        return self._db

    @property
    def current_events(self):
        return self._events_to_write

    def __init__(self, path):

        # Connect to psql
        self._db = psycopg2.connect(os.getenv('DATABASE_URL'))
        cur = self._db.cursor()
        cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('events',))
        if not(cur.fetchone()[0]):
            print('table does not exist')
            cur.execute("CREATE TABLE events (id serial PRIMARY KEY, block_number varchar, transaction_hash varchar, contract_hash varchar, event_type varchar, data jsonb);")
            self._db.commit()
            cur.close()

    def start(self):
        # Handle EventHub events for SmartContract decorators
        self._events_to_write = []

        @events.on(SmartContractEvent.RUNTIME_NOTIFY)
        def call_on_event(sc_event: NotifyEvent):
            self.on_smart_contract_event(sc_event)
            print('EVENT START')

        Blockchain.Default().PersistCompleted.on_change += self.on_persist_completed

    def on_smart_contract_event(self, sc_event: NotifyEvent):
        # Cant use this yet because it doesnt accept all event types
        # if not isinstance(sc_event, NotifyEvent):
        #     logger.info("Not Notify Event instance")
        #     return
        # if sc_event.ShouldPersist:

        self._events_to_write.append(sc_event)

    def on_persist_completed(self, block):
        if len(self._events_to_write):
            for evt in self._events_to_write:  # type:NotifyEvent
                print('THERES AN EVENT!!!')
                print(evt)
                evt.ParsePayload()
                if self.within_script_hash_list(evt.contract_hash):
                    self.write_event_to_psql(evt)

        self._events_to_write = []

    def within_script_hash_list(self, contract_hash):
        print('comparing contract hash white list with:')
        print(contract_hash)
        contract_hashes = os.getenv("CONTRACT_HASH_LIST").split(" ")
        for item in contract_hashes:
            print('iterating contract hashes')
            print(item)
            if str(item) == str(contract_hash):
                print('contract found!')
                return True
        print('contract not found!')
        return False

    def write_event_to_psql(self, event):
        logger.info('WRITING TO PSQL')

        # Prepare variables
        event_type = event.event_payload[0].decode('utf-8')
        event_payload = event.event_payload[1:]
        contract_hash = event.contract_hash
        block_number = event.block_number
        tx_hash = event.tx_hash
        execution_success = event.execution_success
        test_mode = event.test_mode

        data = {'event_payload': event_payload}

        cur = self._db.cursor()

        # cur.execute("CREATE TABLE events (id serial PRIMARY KEY, block_number varchar, transaction_hash varchar, contract_hash varchar, event_type varchar, data jsonb);")

        print('DATA')
        print(data)

        if(execution_success == True):

            cur.execute("INSERT INTO events (block_number, transaction_hash, contract_hash, event_type, data) VALUES (%s, %s, %s, %s, %s)",
                        (str(block_number), str(tx_hash), str(contract_hash), event_type, json.dumps(data)))
        else:
            print('execution failed, not inserting')

        # cur.execute("SELECT * FROM events;")
        # print(cur.fetchall())

        self._db.commit()
        cur.close()
        print('DONE WRITING TO PSQL')
        # conn.close()