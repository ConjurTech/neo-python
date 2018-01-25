import threading
import os
from time import sleep

from logzero import logger
from twisted.internet import reactor, task

from neo.Network.NodeLeader import NodeLeader
from neo.Core.Blockchain import Blockchain
from neo.Implementations.Blockchains.LevelDB.LevelDBBlockchain import LevelDBBlockchain
from neo.Settings import settings
from neo.Implementations.Notifications.Psql.CustomNotificationDB import CustomNotificationDb
from neo.Implementations.Notifications.LevelDB.NotificationDB import NotificationDB

# Create am absolute references to the project root folder. Used for
# specifying the various filenames.
dir_current = os.path.dirname(os.path.abspath(__file__))
DIR_PROJECT_ROOT = os.path.abspath(os.path.join(dir_current))

# The protocol json file
SETTINGS_FILE = os.path.join(DIR_PROJECT_ROOT, 'protocol.json')
# Logfile
settings.set_logfile("/tmp/logfile.log", max_bytes=1e7, backup_count=3)


def log_details():
    while True:
        logger.info("Block %s / %s", str(Blockchain.Default().Height), str(Blockchain.Default().HeaderHeight))
        sleep(2)


def main():
    # Connect to blockchain
    settings.setup(SETTINGS_FILE)

    # Instantiate the blockchain and subscribe to notifications
    blockchain = LevelDBBlockchain(settings.LEVELDB_PATH)
    Blockchain.RegisterBlockchain(blockchain)
    dbloop = task.LoopingCall(Blockchain.Default().PersistBlocks)
    dbloop.start(.1)

    # Try to set up a notification db
    if CustomNotificationDb.instance():
        CustomNotificationDb.instance().start()

    # Start a thread with custom code
    # d = threading.Thread(target=log_details())
    # d.setDaemon(True)  # daemonizing the thread will kill it when the main thread is quit
    # d.start()

    NodeLeader.Instance().Start()
    reactor.run()


if __name__ == "__main__":
    main()
