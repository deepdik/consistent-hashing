import asyncio
from uhashring import HashRing
from motor.motor_asyncio import AsyncIOMotorClient
import time

# MongoDB nodes (simulating multiple MongoDB instances)
mongodb_nodes = [
    {"host": "localhost", "port": 27019},
    {"host": "localhost", "port": 27020},
    {"host": "localhost", "port": 27021}
]

# Initialize HashRing with MongoDB nodes
hash_ring = HashRing(nodes=[f"{node['host']}:{node['port']}" for node in mongodb_nodes])

# Initialize MongoDB clients asynchronously
mongo_clients = {
    f"{node['host']}:{node['port']}": AsyncIOMotorClient(node["host"], node["port"]) for node in mongodb_nodes
}


# Function to get the next node in the hash ring
def get_next_node(node_key):
    nodes = list(hash_ring.nodes)
    current_index = nodes.index(node_key)
    next_index = (current_index + 1) % len(nodes)
    return nodes[next_index]

# Async function to insert documents into MongoDB with backup
async def insert_documents_with_backup(num_docs, start_id=0, batch_size=100000):
    start_time = time.time()
    documents_dict = {node: [] for node in mongo_clients}
    backup_documents_dict = {node: [] for node in mongo_clients}
    key_ranges = {node: {"min": None, "max": None, "count": 0, "backup_count": 0} for node in mongo_clients}

    for i in range(start_id, start_id + num_docs):
        key = str(i)  # Generate unique key
        node = hash_ring.get_node(key)
        next_node = get_next_node(node)  # Get the next node for backup
        print(node, "---", next_node)

        # Create the original document
        document = {"_id": key, "value": f"value_{i}"}
        documents_dict[node].append(document)

        # Create a backup copy of the document (with the same key but `is_backup` set to True)
        backup_document = {"_id": key, "value": f"value_{i}", "is_backup": True}
        backup_documents_dict[next_node].append(backup_document)

        # Update key range and count for the node
        if key_ranges[node]["min"] is None or int(key) < int(key_ranges[node]["min"]):
            key_ranges[node]["min"] = key
        if key_ranges[node]["max"] is None or int(key) > int(key_ranges[node]["max"]):
            key_ranges[node]["max"] = key
        key_ranges[node]["count"] += 1
        key_ranges[next_node]["backup_count"] += 1

        # Insert documents in batches
        if len(documents_dict[node]) == batch_size:
            mongo_client = mongo_clients[node]
            db = mongo_client['mydatabase']
            collection = db['mycollection']
            await collection.insert_many(documents_dict[node])
            documents_dict[node] = []

        if len(backup_documents_dict[next_node]) == batch_size:
            backup_client = mongo_clients[next_node]
            backup_db = backup_client['mydatabase']
            backup_collection = backup_db['mycollection']
            await backup_collection.insert_many(backup_documents_dict[next_node])
            backup_documents_dict[next_node] = []

    # Insert any remaining documents
    for node, documents in documents_dict.items():
        if documents:
            mongo_client = mongo_clients[node]
            db = mongo_client['mydatabase']
            collection = db['mycollection']
            await collection.insert_many(documents)
    for node, backup_documents in backup_documents_dict.items():
        if backup_documents:
            backup_client = mongo_clients[node]
            backup_db = backup_client['mydatabase']
            backup_collection = backup_db['mycollection']
            await backup_collection.insert_many(backup_documents)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Inserted {num_docs} documents in {elapsed_time} seconds")

    # Display key distribution analysis
    print("\nKey Distribution Analysis:")
    for node, range_info in key_ranges.items():
        print(f"Node: {node}")
        print(f"  Min Key: {range_info['min']}")
        print(f"  Max Key: {range_info['max']}")
        print(f"  Total Keys: {range_info['count']}")
        print(f"  Backup Keys: {range_info['backup_count']}")
        print()


# Async function to delete all documents from the collection
async def delete_all_documents():
    try:
        for client in mongo_clients.values():
            db = client['mydatabase']
            collection = db['mycollection']
            result = await collection.delete_many({})
            print(f"Deleted {result.deleted_count} documents from collection '{collection.name}'")
    except Exception as e:
        print(f"Error deleting documents: {e}")


# Function to identify and display main data and backup data on each node
async def identify_main_and_backup_data():
    for node_key, mongo_client in mongo_clients.items():
        db = mongo_client['mydatabase']
        collection = db['mycollection']
        main_count = await collection.count_documents({"is_backup": {"$exists": False}})
        backup_count = await collection.count_documents({"is_backup": True})
        print(f"Node: {node_key} -> Main Data: {main_count}, Backup Data: {backup_count}")


async def find_document(key):
    node = hash_ring.get_node(key)
    mongo_client = mongo_clients.get(node)

    if not mongo_client:
        # If the node is down, get the backup node
        print(f"Node {node} is down, using backup data.")
        node = get_next_node(node)  # Redirect to the backup node
        mongo_client = mongo_clients[node]

    print(f"Querying key '{key}' on node '{node}'")
    db = mongo_client['mydatabase']
    collection = db['mycollection']

    # Attempt to find the document
    document = await collection.find_one({"_id": key})
    if not document:
        print(f"Document with key '{key}' not found")
    else:
        print(document)

# Function to simulate taking down a node
def take_down_node(node_key):
    print(f"\nTaking down node: {node_key}")
    hash_ring.remove_node(node_key)
    mongo_clients.pop(node_key, None)


# Function to restore a node and sync data
async def restore_node_and_sync(node_key):
    print(f"Restoring node: {node_key}")
    hash_ring.add_node(node_key)
    mongo_clients[node_key] = AsyncIOMotorClient(node_key.split(":")[0], int(node_key.split(":")[1]))

    # Get the next node in the hash ring for backup data
    next_node = get_next_node(node_key)
    mongo_client = mongo_clients[node_key]
    db = mongo_client['mydatabase']
    collection = db['mycollection']

    # Get the backup client and collection
    backup_client = mongo_clients[next_node]
    backup_db = backup_client['mydatabase']
    backup_collection = backup_db['mycollection']

    # Sync missing data from backup to the restored node
    backup_documents = backup_collection.find({"is_backup": True})
    async for document in backup_documents:
        original_key = document["_id"]
        if await collection.count_documents({"_id": original_key}) == 0:
            # Copy the missing document from backup to the restored node
            original_document = {"_id": original_key, "value": document["value"]}
            await collection.insert_one(original_document)

    print(f"Data synced to restored node: {node_key}")

    # Re-create backup copies on the next node
    documents = collection.find({"is_backup": {"$exists": False}})
    backup_documents_to_insert = []
    async for document in documents:
        backup_key = document["_id"]
        if await backup_collection.count_documents({"_id": backup_key}) == 0:
            backup_document = {"_id": backup_key, "value": document["value"], "is_backup": True}
            backup_documents_to_insert.append(backup_document)

    if backup_documents_to_insert:
        await backup_collection.insert_many(backup_documents_to_insert)
        print(f"Backup data re-created on node: {next_node}")


# Function to check data consistency
async def check_data_consistency():
    total_main_data = 0
    total_backup_data = 0

    print("\nData Distribution:")
    for node_key, mongo_client in mongo_clients.items():
        db = mongo_client['mydatabase']
        collection = db['mycollection']
        main_data_count = await collection.count_documents({"is_backup": {"$exists": False}})
        backup_data_count = await collection.count_documents({"is_backup": True})
        total_main_data += main_data_count
        total_backup_data += backup_data_count
        print(f"Node: {node_key} -> Main Data: {main_data_count}, Backup Data: {backup_data_count}")

    print(f"\nTotal Main Data: {total_main_data}, Total Backup Data: {total_backup_data}")


# Example usage
async def main():
    # Clean up any previous data
    await delete_all_documents()

    num_documents = 100  # Adjust as needed
    # Insert documents with backup
    await insert_documents_with_backup(num_documents)

    # Identify main and backup data on each node
    await identify_main_and_backup_data()

    # Step 1: Query data from nodes
    await find_document("13")
    await find_document("25")
    await find_document("99")

    # Step 2: Take down a node
    take_down_node("localhost:27020")

    # Step 3: Query data again, should use backup copies
    await find_document("13")
    await find_document("25")
    await find_document("99")

    # Step 5: Restore the node and sync data
    await restore_node_and_sync("localhost:27020")

    await find_document("13")
    await find_document("25")
    await find_document("99")


if __name__ == "__main__":
    asyncio.run(main())
