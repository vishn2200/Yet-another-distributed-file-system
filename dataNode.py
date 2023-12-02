import rpyc
import uuid
import os
import threading
import time
from rpyc.utils.server import ThreadedServer
import sys
import shutil

DATA_DIR = "/home/pes1ug21cs718/Project/root" #root path

class dataNodeervice(rpyc.Service):
    class exposed_data_Node():
        root_folder = "root"  
        nameNode_ip = "localhost"
        nameNode_port = 2131

        def _init_(self):
            self.nameNode_conn = rpyc.connect(self.nameNode_ip, self.nameNode_port)
            self.nameNode = self.nameNode_conn.root.nameNode()
            self.ping_thread_running = False 

        def exposed_put(self, block_uuid, data, dataNode, folder_path=""):
            folder_path = os.path.join(DATA_DIR, folder_path)

            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

            block_path = os.path.join(folder_path, str(block_uuid))

            with open(block_path, 'w') as f:
                f.write(data)

            if len(dataNode) > 0:
                self.forward(block_uuid, data, dataNode, folder_path)

        def exposed_get(self, block_uuid, folder_path=""):
            folder_path = os.path.join(DATA_DIR, folder_path)
            block_path = os.path.join(folder_path, str(block_uuid))

            if not os.path.isfile(block_path):
                return None

            with open(block_path) as f:
                return f.read()

        def exposed_forward(self, block_uuid, data, dataNode, folder_path):
            print("8888: forwarding to:")
            print(block_uuid, dataNode, flush=True) 
            for data_Node in dataNode:
                host, port = data_Node
                con = rpyc.connect(host, port=port)
                data_Node = con.root.data_Node()
                data_Node.put(block_uuid, data, [], folder_path)

        def exposed_ping(self):
            print("Pinging nameNode...")
            self.nameNode.exposed_acknowledge_ping("data_Node here") 

        def exposed_acknowledge_ping(self, data_Node_id):
            print(f"Acknowledging ping from {data_Node_id}")
            self.nameNode.exposed_print_acknowledgment(data_Node_id)  


        def exposed_create_directory(self, folder_path):
            full_path = os.path.join(DATA_DIR, folder_path)
            if not os.path.exists(full_path):
                os.makedirs(full_path)
            else:
                raise Exception("Directory already exists.")

        def exposed_delete(self, path):
            full_path = os.path.join(DATA_DIR, path)
            if os.path.exists(full_path):
                if os.path.isdir(full_path):
                    shutil.rmtree(full_path)
                else:
                    os.remove(full_path)
            else:
                raise Exception("File or directory does not exist.")

        def exposed_move(self, source_path, dest_path):
            source_full_path = os.path.join(DATA_DIR, source_path)
            dest_full_path = os.path.join(DATA_DIR, dest_path)
            if os.path.exists(source_full_path):
                os.rename(source_full_path, dest_full_path)
            else:
                raise Exception("Source path does not exist.")

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

def print_status():
    while True:
        print("Data Node server is running...", flush=True)
        time.sleep(5)

def start_server():
    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)
    t = ThreadedServer(dataNodeervice, port=8888)
    print("Data Node server started on port 8888.")
    threading.Thread(target=print_status, daemon=True).start() 
    t.start()

if __name__ == "__main__":
    start_server()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        sys.exit(0)  