import rpyc
import uuid
import threading
import math
import random
import configparser as ConfigParser
import signal
import pickle
import sys
import os
import time
import shutil

from rpyc.utils.server import ThreadedServer

DATA_DIR = "/home/pes1ug21cs718/Final_Project/" #path to files

def int_handler(signal, frame):
    pickle.dump((nameNodeService.exposed_nameNode.file_table, nameNodeService.exposed_nameNode.block_mapping), open('fs.img', 'wb'))
    sys.exit(0)

def set_conf():
    conf = ConfigParser.ConfigParser()
    conf.read_file(open('dfs.conf'))
    nameNodeService.exposed_nameNode.block_size = int(conf.get('nameNode', 'block_size'))
    nameNodeService.exposed_nameNode.replication_factor = int(conf.get('nameNode', 'replication_factor'))
    dataNode = conf.get('nameNode', 'dataNode').split(',')
    for m in dataNode:
        id, host, port = m.split(":")
        nameNodeService.exposed_nameNode.dataNode[id] = (host, port)

    if os.path.isfile('fs.img'):
        nameNodeService.exposed_nameNode.file_table, nameNodeService.exposed_nameNode.block_mapping = pickle.load(
            open('fs.img', 'rb'))

class nameNodeService(rpyc.Service):
    class exposed_nameNode():
        file_table = {}
        block_mapping = {}
        dataNode = {}

        block_size = 0
        replication_factor = 0

        def exposed_read(self, fname):
            mapping = self.__class__.file_table[fname]
            return mapping

        def exposed_write(self, dest, size):
            if self.exists(dest):
                pass   

            self.__class__.file_table[dest] = []

            num_blocks = self.calc_num_blocks(size)
            blocks = self.alloc_blocks(dest, num_blocks)
            print(f"File Table after write: {self.__class__.file_table}")
            return blocks

        def exposed_get_file_table_entry(self, fname):
            if fname in self.__class__.file_table:
                return self.__class__.file_table[fname]
            else:
                return None

        def exposed_get_block_size(self):
            return self.__class__.block_size

        def exposed_get_dataNode(self):
            return self.__class__.dataNode

        def exposed_check_name_Node_availability(self, name_Node_id):
            name_Node = self.__class__.dataNode.get(name_Node_id)
            if name_Node:
                host, port = name_Node
                try:
                    con = rpyc.connect(host, port=port)
                    name_Node_service = con.root.data_Node()
                    name_Node_service.ping() 
                    return True  
                except Exception as e:
                    return False  
            else:
                return False 

        def exposed_ping_name_Nodes(self):
            while True:
                for name_Node_id in self.__class__.dataNode:
                    if self.exposed_check_name_Node_availability(name_Node_id):
                        con = rpyc.connect(self.__class__.dataNode[name_Node_id][0], port=self.__class__.dataNode[name_Node_id][1])
                        name_Node_service = con.root.data_Node()
                        name_Node_service.exposed_ping() 
                time.sleep(5)  

        def exposed_print_acknowledgment(self, data_Node_id):
            print(f"Received acknowledgment from {data_Node_id}")

        def calc_num_blocks(self, size):
            return int(math.ceil(float(size) / self.__class__.block_size))

        def exists(self, file):
            return file in self.__class__.file_table

        def alloc_blocks(self, dest, num):
            blocks = []
            for i in range(0, num):
                block_uuid = uuid.uuid1()
                nodes_ids = random.sample(list(self.__class__.dataNode.keys()), self.__class__.replication_factor)
                blocks.append((block_uuid, nodes_ids))
                self.__class__.file_table[dest].append((block_uuid, nodes_ids))

            return blocks

        def exposed_list_files(self, folder_path=""):
            folder_path = os.path.join(DATA_DIR, folder_path)

            if not os.path.exists(folder_path):
                return []

            files = []
            for root, dirs, filenames in os.walk(folder_path):
                for file in filenames:
                    files.append(os.path.relpath(os.path.join(root, file), DATA_DIR))

            return files

        def exposed_list_folders(self, folder_path=""):
            folder_path = os.path.join(DATA_DIR, folder_path)

            if not os.path.exists(folder_path):
                return []

            folders = []
            for root, dirs, filenames in os.walk(folder_path):
                for dir in dirs:
                    folders.append(os.path.relpath(os.path.join(root, dir), DATA_DIR) + "/")

            return folders

        def exposed_create_directory(self, folder_path):
            full_path = os.path.join(DATA_DIR, folder_path)
            if not os.path.exists(full_path):
                os.makedirs(full_path)
            else:
                raise Exception("Directory already exists.")

        def exposed_delete(self, path):
         print(f"Received path: {path}")

         full_path = os.path.join(DATA_DIR, path)
         print(f"Deleting: {full_path}")

         if os.path.exists(full_path):
          if os.path.isdir(full_path):
            shutil.rmtree(full_path)
          else:
            os.remove(full_path)

          if path in self.__class__.file_table:
            del self.__class__.file_table[path]
         else:
          raise Exception("File or directory does not exist.")


        def exposed_move(self, source_path, dest_path):
          source_full_path = os.path.join(DATA_DIR, source_path)
          dest_full_path = os.path.join(DATA_DIR, dest_path)
    
          try:
           if os.path.exists(source_full_path):
            os.rename(source_full_path, dest_full_path)
            print(f"File '{source_path}' moved to '{dest_path}' successfully.")
           else:
            raise Exception("Source path does not exist.")
          except Exception as e:
           print(f"Error moving file: {e}")
           raise   


        def exposed_copy(self, source_path, dest_path):
            source_full_path = os.path.join(DATA_DIR, source_path)
            dest_full_path = os.path.join(DATA_DIR, dest_path)
            if os.path.exists(source_full_path):
                if os.path.isdir(source_full_path):
                    shutil.copytree(source_full_path, dest_full_path)
                else:
                    shutil.copy2(source_full_path, dest_full_path)
            else:
                raise Exception("Source path does not exist.")

if __name__ == "__main__":
    set_conf()
    signal.signal(signal.SIGINT, int_handler)
    t = ThreadedServer(nameNodeService, port=2131)
    ping_t = threading.Thread(target=nameNodeService.exposed_nameNode().exposed_ping_name_Nodes)
    ping_t.daemon = True
    ping_t.start()
    t.start()
