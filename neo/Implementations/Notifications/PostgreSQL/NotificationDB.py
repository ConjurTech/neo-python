import psycopg2
import json
import os
import datetime

from neo.EventHub import events
from neo.SmartContract.SmartContractEvent import SmartContractEvent, NotifyEvent
from neo.Settings import settings
from neo.Core.Blockchain import Blockchain
from logzero import logger


class NotificationDB:

    __instance = None

    _events_to_write = None

    @staticmethod
    def instance():
        if not NotificationDB.__instance:
            NotificationDB.__instance = NotificationDB(settings.NOTIFICATION_DB_PATH)
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

    def __init__(self, _path):

        # Connect to psql
        self._db = psycopg2.connect(os.getenv('DATABASE_URL'))
        cur = self._db.cursor()
        # NOTE: ensure uuid-ossp module is installed and enabled with `CREATE EXTENSION uuid-ossp;`;
        cur.execute("CREATE TABLE IF NOT EXISTS events (id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), block_number INTEGER, transaction_hash VARCHAR, contract_hash VARCHAR, event_type VARCHAR, event_payload JSONB, event_time TIMESTAMP);")
        self._db.commit()
        cur.close()

    def start(self):
        self._events_to_write = []

        @events.on(SmartContractEvent.RUNTIME_NOTIFY)
        def call_on_event(sc_event: NotifyEvent):
            self.on_smart_contract_event(sc_event)

        Blockchain.Default().PersistCompleted.on_change += self.on_persist_completed

    def on_smart_contract_event(self, sc_event: NotifyEvent):
        self._events_to_write.append(sc_event)

    def on_persist_completed(self, block):
        for evt in self._events_to_write:
            evt.ParsePayload()
            if self.within_script_hash_list(str(evt.contract_hash)):
                self.write_event_to_psql(evt, block)

        self._events_to_write = []

    def within_script_hash_list(self, contract_hash):
        contract_hashes = os.getenv("CONTRACT_HASH_LIST").split(" ")
        return contract_hash in contract_hashes

    def write_event_to_psql(self, event, block):
        if not event.execution_success or event.test_mode:
            return
        logger.info("Writing event to psql: %s" % event)
        # Prepare variables
        event_type = event.event_payload[0].decode('utf-8')
        event_payload = event.event_payload[1:]
        contract_hash = event.contract_hash
        block_number = event.block_number
        tx_hash = event.tx_hash

        cur = self._db.cursor()
        cur.execute(
            "INSERT INTO events (block_number, transaction_hash, contract_hash, event_type, event_payload, event_time) VALUES (%s, %s, %s, %s, %s, %s)",
            (block_number, str(tx_hash), str(contract_hash), event_type, json.dumps(event_payload), datetime.datetime.fromtimestamp(block.Timestamp)))

        self._db.commit()
        cur.close()
