import asyncio
import uuid
import time
from motor.motor_asyncio import AsyncIOMotorClient

# Initialize MongoDB client with authentication
mongo_uri = "mongodb://localhost:27018/"
client = AsyncIOMotorClient(mongo_uri)
db = client['mydatabase']
collection = db['mycollection']


# Function to insert documents into MongoDB using async/await
async def insert_documents(num_docs, batch_size=100000):
    start_time = time.time()
    documents = []
    tasks = []


    # Count total documents
    total_docs = await collection.count_documents({})
    print(f"Total documents in collection: {total_docs}")

    print("\nStarted inserting data in database.....")

    for i in range(1, num_docs):
        # key = str(uuid.uuid4())  # Generate unique key
        document = {"_id": i, "value": f"value_{i}"}
        documents.append(document)

        if len(documents) == batch_size:
            tasks.append(insert_documents_in_batch(documents))
            documents = []

    if documents:
        tasks.append(insert_documents_in_batch(documents))

    await asyncio.gather(*tasks)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Inserted {num_docs} documents in {elapsed_time} seconds")

    # Count total documents
    total_docs = await collection.count_documents({})
    print(f"Total documents in collection: {total_docs}")


# Function to insert a batch of documents
async def insert_documents_in_batch(documents):
    await collection.insert_many(documents)
    print(f"Inserted {len(documents)} documents")


# Function to find document by key
async def find_document(key):
    start_time = time.time()
    document = await collection.find_one({"_id": key})
    end_time = time.time()
    elapsed_time = end_time - start_time
    if document:
        print(f"Document with key '{key}' found in {elapsed_time} seconds: {document}")
    else:
        print(f"Document with key '{key}' not found")


# Function to remove all documents from the collection (cleanup)
async def cleanup_documents():
    await collection.delete_many({})
    print("All documents have been removed from the collection")


# Example: Inserting and finding documents
async def main():
    num_documents = 1000000  # Adjust as needed, e.g., 10 million documents

    print("clean any previous data")
    await cleanup_documents()
    # Insert documents into MongoDB asynchronously
    await insert_documents(num_documents)

    # Find a document by key (simulate retrieval)
    example_key = 999999
    await find_document(example_key)

    # Clean up the collection
    await cleanup_documents()

# Run the asyncio event loop
if __name__ == "__main__":
    asyncio.run(main())
