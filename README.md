
# Consistent Hashing and Backup Data Management Using MongoDB

This project demonstrates the use of consistent hashing and backup data management in a distributed MongoDB environment using Python and asynchronous operations.

## Overview

The project consists of three test scripts that demonstrate various scenarios:
1. **Single Node Operations**: Inserting and querying data on a single MongoDB instance.
2. **Consistent Hashing Across Multiple Nodes**: Distributing data across multiple MongoDB nodes using consistent hashing.
3. **Consistent Hashing with Backup Data**: Extending the consistent hashing functionality to create backup copies of data on neighboring nodes to provide fault tolerance.

## Prerequisites

- **Docker**: Ensure Docker and Docker Compose are installed on your system.
- **Python**: Make sure Python 3.8 or later is installed.
- **MongoDB**: The setup requires MongoDB instances running on different ports.

## Setup

### Docker Compose

Each test has an associated `docker-compose.yml` file to set up the MongoDB instances. Make sure to start the MongoDB instances using Docker before running the scripts.

1. **Navigate to the appropriate test folder** (e.g., `single-node`, `multinode`, or `ring-with-backup`).
2. **Run the following command** to start the MongoDB instances:

   ```bash
   docker-compose up -d
   ```

3. **Stop the services** once you're done:

   ```bash
   docker-compose down
   ```

## Scripts

### 1. Single Node Operations

- **File**: `single_node_test.py`
- **Description**: This script inserts a large number of documents into a single MongoDB instance and then retrieves a specific document to measure the efficiency of data retrieval.
- **Usage**:

  ```bash
  python single_node_test.py
  ```

### 2. Consistent Hashing Across Multiple Nodes

- **File**: `multinode_test.py`
- **Description**: This script demonstrates consistent hashing by distributing documents across three MongoDB nodes. It also includes functionality to remove a node and redistribute the data.
- **Usage**:

  ```bash
  python multinode_test.py
  ```

- **Features**:
  - Inserts documents into the MongoDB nodes using consistent hashing.
  - Removes a node and migrates the data to other nodes.
  - Adds a node back and redistributes data.

### 3. Ring with Backup Data

- **File**: `ring_with_backup.py`
- **Description**: This script extends the consistent hashing approach by creating backup copies of the data on the next node in the hash ring. If a node goes down, the data can still be accessed from the backup node.
- **Usage**:

  ```bash
  python ring_with_backup.py
  ```

- **Features**:
  - Inserts documents into the MongoDB nodes with backup copies.
  - Simulates a node failure and uses backup data to serve queries.
  - Restores the failed node and synchronizes the data.

## Directory Structure

```plaintext
.
├── single-node
│   ├── docker-compose.yml
│   ├── single_node_test.py
├── multinode
│   ├── docker-compose.yml
│   ├── multinode_test.py
├── ring-with-backup
│   ├── docker-compose.yml
│   ├── ring_with_backup.py
└── README.md
```

## Running the Tests

1. **Single Node Test**:
   - Navigate to the `single-node` folder.
   - Start MongoDB using Docker Compose:

     ```bash
     docker-compose up -d
     ```

   - Run the script:

     ```bash
     python single_node_test.py
     ```

   - Clean up:

     ```bash
     docker-compose down
     ```

2. **Consistent Hashing Test**:
   - Navigate to the `multinode` folder.
   - Start MongoDB using Docker Compose:

     ```bash
     docker-compose up -d
     ```

   - Run the script:

     ```bash
     python multinode_test.py
     ```

   - Clean up:

     ```bash
     docker-compose down
     ```

3. **Consistent Hashing with Backup Test**:
   - Navigate to the `ring-with-backup` folder.
   - Start MongoDB using Docker Compose:

     ```bash
     docker-compose up -d
     ```

   - Run the script:

     ```bash
     python ring_with_backup.py
     ```

   - Clean up:

     ```bash
     docker-compose down
     ```

## Notes

- **Adjust the Number of Documents**: You can change the `num_documents` variable in the scripts to adjust the number of documents being inserted.
- **MongoDB URI**: Update the `mongo_uri` if you're using a different setup or authentication mechanism.
- **Performance**: The scripts use asynchronous operations to maximize performance when handling a large number of documents.

## Troubleshooting

- **MongoDB Connection Issues**: Ensure the MongoDB instances are running and accessible.
- **Duplicate Key Errors**: If you encounter duplicate key errors, ensure the `_id` field values are unique or handle them appropriately in your code.
