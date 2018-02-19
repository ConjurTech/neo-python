import threading
import os
from time import sleep

from logzero import logger
from twisted.internet import reactor, task

from neo.Network.NodeLeader import NodeLeader
from neo.Core.Blockchain import Blockchain
from neo.Implementations.Blockchains.LevelDB.LevelDBBlockchain import LevelDBBlockchain
from neo.Settings import settings
from neo.Implementations.Notifications.PostgreSQL.NotificationDB import NotificationDB

# Create am absolute references to the project root folder. Used for
# specifying the various filenames.
dir_current = os.path.dirname(os.path.abspath(__file__))
DIR_PROJECT_ROOT = os.path.abspath(os.path.join(dir_current))

# The protocol json file
SETTINGS_FILE = os.path.join(DIR_PROJECT_ROOT, os.getenv('PROTOCOL_JSON_FILE'))

# Logfile
settings.set_logfile("/tmp/logfile.log", max_bytes=1e7, backup_count=3)

def main():
    # Connect to blockchain
    settings.setup(SETTINGS_FILE)

    # Instantiate the blockchain and subscribe to notifications
    blockchain = LevelDBBlockchain(settings.LEVELDB_PATH)
    Blockchain.RegisterBlockchain(blockchain)
    dbloop = task.LoopingCall(Blockchain.Default().PersistBlocks)
    dbloop.start(.1)

    # Try to set up a notification db
    if NotificationDB.instance():
        NotificationDB.instance().start()

    NodeLeader.Instance().Start()
    reactor.run()


if __name__ == "__main__":
    main()
