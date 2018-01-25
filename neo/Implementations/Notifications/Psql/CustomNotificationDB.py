import plyvel
from logzero import logger
from neo.EventHub import events
from neo.SmartContract.SmartContractEvent import SmartContractEvent, NotifyEvent, NotifyType
from neo.Settings import settings
from neo.Core.Blockchain import Blockchain
from neo.Core.Helper import Helper
from neocore.UInt160 import UInt160
from custom_database import CustomDatabase


class NotificationPrefix():

    PREFIX_ADDR = b'\xCA'
#    PREFIX_CONTRACT = b'\xCB'
    PREFIX_BLOCK = b'\xCC'

    PREFIX_COUNT = b'\xCD'


class CustomNotificationDb():

    __instance = None

    _events_to_write = None

    @staticmethod
    def instance():
        if not CustomNotificationDb.__instance:
            if settings.NOTIFICATION_DB_PATH:
                CustomNotificationDb.__instance = CustomNotificationDb(settings.NOTIFICATION_DB_PATH)
#                logger.info("Created Notification DB At %s " % settings.NOTIFICATION_DB_PATH)
            else:
                logger.info("Notification DB Path not configured in settings")
        return CustomNotificationDb.__instance

    @staticmethod
    def close():
        if CustomNotificationDb.__instance:
            CustomNotificationDb.__instance.db.close()
            CustomNotificationDb.__instance = None

    @property
    def db(self):
        return self._db

    @property
    def current_events(self):
        return self._events_to_write

    def __init__(self, path):

        try:
            self._db = plyvel.DB(path, create_if_missing=True)
            logger.info("running custom notification db")
            logger.info("Created Notification DB At %s " % path)
        except Exception as e:
            logger.info("Notification leveldb unavailable, you may already be running this process: %s " % e)
            raise Exception('Notification Leveldb Unavailable %s ' % e)

    def start(self):
        # Handle EventHub events for SmartContract decorators
        self._events_to_write = []

        @events.on(SmartContractEvent.RUNTIME_NOTIFY)
        def call_on_event(sc_event: NotifyEvent):
            print('theres an event!!!!!!')
            print(sc_event)
            cd = CustomDatabase()
            cd.write_event_to_psql(sc_event)
            self.on_smart_contract_event(sc_event)
#            elif sc_event.notify_type == NotifyType.TRANSFER and sc_event.test_mode:
#                data = sc_event.ToByteArray()
#                event = SmartContractEvent.FromByteArray(data)
#                print("event? %s " % event)

        Blockchain.Default().PersistCompleted.on_change += self.on_persist_completed

    def on_smart_contract_event(self, sc_event: NotifyEvent):
        if not isinstance(sc_event, NotifyEvent):
            logger.info("Not Notify Event instance")
            return
        if sc_event.ShouldPersist and sc_event.notify_type == NotifyType.TRANSFER:
            self._events_to_write.append(sc_event)

    def on_persist_completed(self, block):
        if len(self._events_to_write):

            addr_db = self.db.prefixed_db(NotificationPrefix.PREFIX_ADDR)
            block_db = self.db.prefixed_db(NotificationPrefix.PREFIX_BLOCK)

            block_write_batch = block_db.write_batch()

            block_count = 0
            block_bytes = self._events_to_write[0].block_number.to_bytes(4, 'little')

            for evt in self._events_to_write:  # type:NotifyEvent
                print('EVENT CAUGHT')
                print('EVENT START')
                print(evt)
                print('EVENT END')


        self._events_to_write = []

    def get_by_block(self, block_number):

        blocklist_snapshot = self.db.prefixed_db(NotificationPrefix.PREFIX_BLOCK).snapshot()
        block_bytes = block_number.to_bytes(4, 'little')
        results = []
        for val in blocklist_snapshot.iterator(prefix=block_bytes, include_key=False):
            event = SmartContractEvent.FromByteArray(val)
            results.append(event)

        return results

    def get_by_addr(self, address):
        addr = address
        if isinstance(address, str) and len(address) == 34:
            addr = Helper.AddrStrToScriptHash(address)

        if not isinstance(addr, UInt160):
            raise Exception("Incorrect address format")

        addrlist_snapshot = self.db.prefixed_db(NotificationPrefix.PREFIX_ADDR).snapshot()
        results = []

        for val in addrlist_snapshot.iterator(prefix=bytes(addr.Data), include_key=False):
            if len(val) > 4:
                try:
                    event = SmartContractEvent.FromByteArray(val)
                    results.append(event)
                except Exception as e:
                    logger.info("could not parse event: %s " % val)
        return results
