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


# Async function to insert documents into MongoDB using bulk insert
# Modified code to count total documents asynchronously
# Updated async function to insert documents and analyze key distribution
async def insert_documents(num_docs, batch_size=100000):
    start_time = time.time()
    documents_dict = {node: [] for node in mongo_clients}
    key_ranges = {node: {"min": None, "max": None, "count": 0} for node in mongo_clients}

    for i in range(num_docs):
        key = str(i)  # Generate unique key
        node = hash_ring.get_node(key)
        documents_dict[node].append({"_id": key, "value": f"value_{i}"})

        # Update key range and count for the node
        if key_ranges[node]["min"] is None or int(key) < int(key_ranges[node]["min"]):
            key_ranges[node]["min"] = key
        if key_ranges[node]["max"] is None or int(key) > int(key_ranges[node]["max"]):
            key_ranges[node]["max"] = key
        key_ranges[node]["count"] += 1

        if len(documents_dict[node]) == batch_size:
            mongo_client = mongo_clients[node]
            db = mongo_client['mydatabase']
            collection = db['mycollection']
            await collection.insert_many(documents_dict[node])
            documents_dict[node] = []
            print(f"Inserted {i + 1} / {num_docs} documents")

    # Insert any remaining documents
    for node, documents in documents_dict.items():
        if documents:
            mongo_client = mongo_clients[node]
            db = mongo_client['mydatabase']
            collection = db['mycollection']
            await collection.insert_many(documents)
            print(f"Inserted {num_docs} / {num_docs} documents")

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


# Async function to find a document by key
async def find_document(key):
    start_time = time.time()
    node = hash_ring.get_node(key)
    print(f'This is the node {node}')
    mongo_client = mongo_clients[node]
    db = mongo_client['mydatabase']
    collection = db['mycollection']
    document = await collection.find_one({"_id": key})
    end_time = time.time()
    elapsed_time = end_time - start_time
    if document:
        print(f"Document with key '{key}' found in {elapsed_time} seconds: {document}")
    else:
        print(f"Document with key '{key}' not found")


# Function to get the next node in the hash ring in clockwise direction
def get_next_node(node_key):
    """Get the next node in the hash ring in clockwise direction."""
    if not hash_ring.sorted_keys:
        return None

    # Calculate the hash value for the given node_key
    hash_value = hash_ring._hash_function(node_key)

    # Find the next hash key in the sorted list
    for hash_key in hash_ring.sorted_keys:
        if hash_value < hash_key:
            return hash_ring.ring[hash_key]

    # If no larger hash key is found, wrap around to the first node
    return hash_ring.ring[hash_ring.sorted_keys[0]]


# Optimized async function to remove a node and migrate data efficiently
async def remove_node_and_migrate_data(host, port, batch_size=100000):
    node_key = f"{host}:{port}"
    hash_ring.remove_node(node_key)  # Remove the node from the HashRing
    mongo_client = mongo_clients.pop(node_key, None)  # Remove the MongoDB client

    if mongo_client:
        print(f"Removing node: {node_key} and redistributing data among remaining nodes")
        try:
            db = mongo_client['mydatabase']
            collection = db['mycollection']

            # Retrieve all documents from the removed node
            total_docs = await collection.count_documents({})
            print(f"Total documents to transfer: {total_docs}")

            if total_docs > 0:
                # Use an indexed field (_id) for efficient pagination
                last_id = None

                while True:
                    query = {"_id": {"$gt": last_id}} if last_id else {}
                    documents = await collection.find(query).limit(batch_size).to_list(batch_size)

                    if not documents:
                        break

                    # Determine the new nodes for each document and group them for batch insertion
                    grouped_documents = {}
                    for document in documents:
                        key = document["_id"]
                        new_node = hash_ring.get_node(key)  # Get the new node for the document
                        if new_node not in grouped_documents:
                            grouped_documents[new_node] = []
                        grouped_documents[new_node].append(document)

                    # Batch insert documents into the new nodes
                    for new_node, docs in grouped_documents.items():
                        new_mongo_client = mongo_clients[new_node]
                        new_db = new_mongo_client['mydatabase']
                        new_collection = new_db['mycollection']
                        await new_collection.insert_many(docs)

                    # Batch delete documents from the old node
                    keys_to_delete = [doc["_id"] for doc in documents]
                    await collection.delete_many({"_id": {"$in": keys_to_delete}})

                    # Update last_id for pagination
                    last_id = documents[-1]["_id"]

                    print(f"Transferred batch up to _id: {last_id}")

                print(f"Transferred {total_docs} documents to appropriate nodes")

            # Clean up the removed node's collection
            await collection.delete_many({})

        except Exception as e:
            print(f"Error migrating documents: {e}")
    else:
        print(f"Node {node_key} was not found in the cluster.")


# Function to get the previous node in the hash ring in counter-clockwise direction
def get_previous_node(node_key):
    nodes = list(hash_ring.nodes)
    current_index = nodes.index(node_key)
    previous_index = (current_index - 1 + len(nodes)) % len(nodes)
    return nodes[previous_index]


async def add_node_and_migrate_data(host, port, batch_size=10000):
    new_node_key = f"{host}:{port}"
    hash_ring.add_node(new_node_key)
    mongo_clients[new_node_key] = AsyncIOMotorClient(host, port)
    print(f"Added new node: {new_node_key}")

    for current_node_key, mongo_client in mongo_clients.items():
        if current_node_key == new_node_key:
            continue  # Skip the new node

        db = mongo_client['mydatabase']
        collection = db['mycollection']

        print(f"Analyzing documents from {current_node_key} for migration to {new_node_key}...")

        total_docs = await collection.count_documents({})
        print(f"Total documents on {current_node_key}: {total_docs}")

        last_id = None
        migrated_count = 0

        try:
            while True:
                # Fetch documents in batches
                query = {"_id": {"$gt": last_id}} if last_id else {}
                documents = await collection.find(query).limit(batch_size).to_list(batch_size)

                if not documents:
                    break

                # Filter documents that need to be migrated to the new node
                docs_to_migrate = [
                    doc for doc in documents if hash_ring.get_node(doc["_id"]) == new_node_key
                ]

                if docs_to_migrate:
                    # Insert into the new node
                    new_mongo_client = mongo_clients[new_node_key]
                    new_db = new_mongo_client['mydatabase']
                    new_collection = new_db['mycollection']

                    try:
                        await new_collection.insert_many(docs_to_migrate)
                        migrated_count += len(docs_to_migrate)
                    except Exception as e:
                        print(f"Error inserting documents to {new_node_key}: {e}")
                        continue

                    # Delete successfully migrated documents from the source node
                    keys_to_delete = [doc["_id"] for doc in docs_to_migrate]
                    try:
                        await collection.delete_many({"_id": {"$in": keys_to_delete}})
                    except Exception as e:
                        print(f"Error deleting documents from {current_node_key}: {e}")

                # Update last_id for pagination
                last_id = documents[-1]["_id"]

                print(f"Migrated batch up to _id: {last_id}")

            print(f"Finished migrating {migrated_count} documents from {current_node_key} to {new_node_key}")

        except Exception as e:
            print(f"Error during migration from {current_node_key} to {new_node_key}: {e}")




async def debug_key_assignments():
    for key in range(0, 100):  # Use a smaller range for debugging
        node = hash_ring.get_node(str(key))
        print(f"Key: {key} -> Node: {node}")


# Function to analyze key distribution over a larger range
async def analyze_key_distribution(num_docs):
    key_counts = {node: 0 for node in mongo_clients}

    for i in range(num_docs):
        key = str(i)
        node = hash_ring.get_node(key)
        key_counts[node] += 1

    # Print key distribution summary
    print("\nKey Distribution Summary:")
    for node, count in key_counts.items():
        print(f"Node: {node} -> Total Keys: {count}")


async def data_distribution():
    # Verify document counts across all nodes
    for node_key, mongo_client in mongo_clients.items():
        db = mongo_client['mydatabase']
        collection = db['mycollection']
        total_docs = await collection.count_documents({})
        print(f"Node: {node_key} -> Total Keys: {total_docs}")


# Example: Inserting and finding documents
async def main():
    # Run the debug function

    # clean any previous data
    await delete_all_documents()

    num_documents = 100000  # Adjust as needed
    # Insert documents into MongoDB asynchronously
    await insert_documents(num_documents)

    await analyze_key_distribution(num_documents)
    print("Data distribution:")
    await data_distribution()
    time.sleep(2)
    # Find a document by key (simulate retrieval)
    example_key = "2000"  # Replace with an actual key
    await find_document(example_key)
    example_key = "3000"  # Replace with an actual key
    await find_document(example_key)
    example_key = "99999"  # Replace with an actual key
    await find_document(example_key)

    time.sleep(2)
    # Remove the node on port 27020 and migrate its data
    await remove_node_and_migrate_data("localhost", 27020)

    print("Data distribution:")
    await data_distribution()

    # Verify that data has been migrated correctly
    # Find a document by key (simulate retrieval)
    time.sleep(2)
    example_key = "2000"  # Replace with an actual key
    await find_document(example_key)
    example_key = "3000"  # Replace with an actual key
    await find_document(example_key)
    example_key = "99999"  # Replace with an actual key
    await find_document(example_key)

    await add_node_and_migrate_data("localhost", 27020)
    time.sleep(2)
    print("Data distribution:")
    await data_distribution()


    example_key = "2000"  # Replace with an actual key
    await find_document(example_key)
    example_key = "3000"  # Replace with an actual key
    await find_document(example_key)
    example_key = "99999"  # Replace with an actual key
    await find_document(example_key)



    # Delete all documents (clean up)
    await delete_all_documents()


# Run the asyncio event loop
if __name__ == "__main__":
    asyncio.run(main())

