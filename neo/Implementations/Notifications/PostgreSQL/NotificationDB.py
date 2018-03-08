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
        # NOTE: ensure uuid-ossp module is installed and enabled with `CREATE EXTENSION "uuid-ossp";`;
        cur.execute(
            "CREATE TABLE IF NOT EXISTS events ("
            "id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), "
            "block_number INTEGER, "
            "transaction_hash VARCHAR, "
            "contract_hash VARCHAR, "
            "event_type VARCHAR, "
            "event_payload JSONB, "
            "event_time TIMESTAMP, "
            "blockchain VARCHAR);")

        cur.execute(
            "CREATE TABLE IF NOT EXISTS offers ("
            "id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), "
            "block_number INTEGER, "
            "transaction_hash VARCHAR, "
            "contract_hash VARCHAR, "
            "offer_time TIMESTAMP, "
            "blockchain VARCHAR, "
            "address VARCHAR, "
            "offer_hash VARCHAR, "
            "offer_asset_id VARCHAR, "
            "offer_amount BIGINT, "
            "want_asset_id VARCHAR, "
            "want_amount BIGINT);")

        cur.execute(
            "CREATE TABLE IF NOT EXISTS trades ("
            "id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), "
            "block_number INTEGER, "
            "transaction_hash VARCHAR, "
            "contract_hash VARCHAR, "
            "event_type VARCHAR, "
            "address VARCHAR, "
            "offer_hash VARCHAR, "
            "filled_amount BIGINT, "
            "offer_asset_id VARCHAR, "
            "offer_amount BIGINT, "
            "want_asset_id VARCHAR, "
            "want_amount BIGINT, "
            "event_time TIMESTAMP, "
            "blockchain VARCHAR);")

        cur.execute(
            "CREATE TABLE IF NOT EXISTS orders ("
            "id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), "
            "transaction_hash VARCHAR, "
            "contract_hash VARCHAR, "
            "blockchain VARCHAR, ")

        cur.execute(
            "CREATE TABLE IF NOT EXISTS pending_offers ("
            "id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), "
            "order VARCHAR references orders(order)"
            "block_number INTEGER, "
            "transaction_hash VARCHAR, "
            "contract_hash VARCHAR, "
            "offer_time TIMESTAMP, "
            "blockchain VARCHAR, "
            "offer_asset_id VARCHAR, "
            "offer_amount BIGINT, "
            "want_asset_id VARCHAR, "
            "want_amount BIGINT);")

        cur.execute(
            "CREATE TABLE IF NOT EXISTS pending_fills ("
            "id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), "
            "order VARCHAR references orders(order)"
            "pending_offer VARCHAR references pending_offers(pending_offer)"
            "block_number INTEGER, "
            "transaction_hash VARCHAR, "
            "contract_hash VARCHAR, "
            "filled_amount BIGINT, "
            "offer_asset_id VARCHAR, "
            "offer_amount BIGINT, "
            "want_asset_id VARCHAR, "
            "want_amount BIGINT, "
            "event_time TIMESTAMP, "
            "blockchain VARCHAR);")

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
        blockchain = 'neo'

        cur = self._db.cursor()

        cur.execute(
            "INSERT INTO events ("
            "block_number, transaction_hash, contract_hash, event_type, event_payload, event_time, blockchain)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (block_number, str(tx_hash), str(contract_hash), event_type, json.dumps(event_payload),
             datetime.datetime.fromtimestamp(block.Timestamp), blockchain))

        if event_type == "filled":
            address = event_payload[0]
            offer_hash = event_payload[1]
            filled_amount = event_payload[2]
            offer_asset_id = event_payload[3]
            offer_amount = event_payload[4]
            want_asset_id = event_payload[5]
            want_amount = event_payload[6]

            cur.execute(
                "INSERT INTO trades ("
                "block_number, transaction_hash, contract_hash, event_type, address, offer_hash, filled_amount, "
                "offer_asset_id, offer_amount, want_asset_id, want_amount, event_time, blockchain)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (block_number, str(tx_hash), str(contract_hash), event_type, address, offer_hash, filled_amount,
                 offer_asset_id, offer_amount, want_asset_id, want_amount,
                 datetime.datetime.fromtimestamp(block.Timestamp), blockchain))

        if event_type == "created":
            offer_hash = event_payload[0]
            offer_asset_id = event_payload[1]
            offer_amount = event_payload[2]
            want_asset_id = event_payload[3]
            want_amount = event_payload[4]

            # Search orders for same tx hash
            cur.execute("SELECT id FROM orders WHERE transaction_hash = %s", str(tx_hash))
            order_id = cur.fetchall()

            # Insert order if none with same tx hash
            if order_id is None:
                cur.execute("INSERT INTO orders (transaction_hash, contract_hash, blockchain)"
                            "VALUES(%s, %s, %s)",
                            str(tx_hash), str(contract_hash), blockchain)

            # Create pending offer with tx hash of the order
            cur.execute(
                "INSERT INTO pending_offers ("
                "block_number, transaction_hash, contract_hash, offer_time,"
                "blockchain, offer_asset_id, offer_amount, want_asset_id, want_amount)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (block_number, str(tx_hash), str(contract_hash), datetime.datetime.fromtimestamp(block.Timestamp),
                 blockchain, offer_asset_id, offer_amount, want_asset_id, want_amount)

            )

        # if event_type == "created":
        #
        #     address = event_payload[0]
        #     offer_hash = event_payload[1]
        #     offer_asset_id = event_payload[2]
        #     offer_amount = event_payload[3]
        #     want_asset_id = event_payload[4]
        #     want_amount = event_payload[5]
        #
        #     cur.execute(
        #         "INSERT INTO offers ("
        #         "block_number, transaction_hash, contract_hash, offer_time,"
        #         "blockchain, address, offer_hash, offer_asset_id, offer_amount, want_asset_id, want_amount)"
        #         " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        #         (block_number, str(tx_hash), str(contract_hash), datetime.datetime.fromtimestamp(block.Timestamp),
        #          blockchain, address, offer_hash, offer_asset_id, offer_amount, want_asset_id, want_amount)
        #     )

        self._db.commit()
        cur.close()
