import rpyc
import sys
import os
import logging

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)

class DFSClient:
    def __init__(self, nameNode_host="localhost", nameNode_port=2131):
        self.nameNode_conn = rpyc.connect(nameNode_host, nameNode_port)
        self.nameNode = self.nameNode_conn.root.nameNode()

    def send_to_data_Node(self, block_uuid, data, dataNode, folder_path=""):
        LOG.info("Sending: " + str(block_uuid) + str(dataNode))
        for data_Node in dataNode:
            host, port = data_Node
            con = rpyc.connect(host, port=port)
            data_Node = con.root.data_Node()
            data_Node.put(block_uuid, data, [], folder_path)

    def create_directory(self, folder_path):
        try:
            self.nameNode.create_directory(folder_path)
            print(f"Directory '{folder_path}' created successfully.")
        except Exception as e:
            print(f"Error creating directory: {e}")

    def delete_file_entry(self, file_name, folder_path=""):
        file_path = os.path.join(folder_path, file_name)
        try:
            self.nameNode.delete(file_path)
            self.nameNode.exposed_delete(file_name, folder_path)
            print(f"File entry '{file_path}' deleted from the file table successfully.")
        except Exception as e:
            #print(f"Error deleting file entry: {e}")
            pass

    def move(self, source_path, dest_path):
        try:
            self.nameNode.move(source_path, dest_path)
            print(f"File or directory '{source_path}' moved to '{dest_path}' successfully.")
        except Exception as e:
            print(f"Error moving file or directory: {e}")

    def copy(self, source_path, dest_path):
        try:
            self.nameNode.copy(source_path, dest_path)
            print(f"File or directory '{source_path}' copied to '{dest_path}' successfully.")
        except Exception as e:
            print(f"Error copying file or directory: {e}")

    def read_from_data_Node(self, block_uuid, data_Node, folder_path=""):
        host, port = data_Node
        con = rpyc.connect(host, port=port)
        data_Node = con.root.data_Node()
        return data_Node.get(block_uuid, folder_path)

    def list_files_and_folders(self, folder_path=""):
        files = self.nameNode.list_files(folder_path)
        folders = self.nameNode.list_folders(folder_path)

        print("Files:")
        for file in files:
            print(file)

        print("\nFolders:")
        for folder in folders:
            print(folder)

    def get(self, fname, folder_path=""):
        file_table = self.nameNode.get_file_table_entry(os.path.join(folder_path, fname))
        if not file_table:
            LOG.info("404: file not found")
            return

        for block in file_table:
            for m in [self.nameNode.get_dataNode()[_] for _ in block[1]]:
                data = self.read_from_data_Node(block[0], m, folder_path)
                if data:
                    sys.stdout.write(data)
                    break
            else:
                LOG.info("No blocks found. Possibly a corrupt file")

    def put(self, source, dest, folder_path=""):
        size = os.path.getsize(source)
        blocks = self.nameNode.write(os.path.join(folder_path, dest), size)
        with open(source) as f:
            for b in blocks:
                data = f.read(self.nameNode.get_block_size())
                block_uuid = b[0]
                dataNode = [self.nameNode.get_dataNode()[_] for _ in b[1]]
                self.send_to_data_Node(block_uuid, data, dataNode, folder_path)

    def close_connections(self):
        self.nameNode_conn.close()

def main(args):
    if len(args) < 1:
        print("Usage: python client.py [get/put/list/mkdir/delete/move/copy] [arguments]")
        return
    client = DFSClient()

    if args[0] == "get":
        client.get(args[1],args[2] if len(args) == 3 else "")
    elif args[0] == "put":
        client.put(args[1], args[2], args[3] if len(args) == 4 else "")
    elif args[0] == "list":
        if len(args) == 2:
            client.list_files_and_folders(args[1])
        else:
            client.list_files_and_folders()
    elif args[0] == "mkdir":
        current_path = os.getcwd()
        directory_path = os.path.join(current_path, "root")
        if os.path.exists(directory_path) and os.path.isdir(directory_path) and args[1]=="root":
            print("root folder doesn't exist and must be created")
            client.create_directory(args[1])
        else:
            client.create_directory(f"root/{args[1]}")
    elif args[0] == "delete":
        client.delete_file_entry(args[1], args[2] if len(args) == 3 else "")
    elif args[0] == "move":
        client.move(args[1],args[2])
    elif args[0] == "copy":
        client.copy(args[1], args[2])
    else:
        LOG.error("Usage: client.py [get/put/list/mkdir/delete/move/copy] [arguments]")

    client.close_connections()

if __name__ == "__main__":
    main(sys.argv[1:])