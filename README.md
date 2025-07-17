# CLFS (Claim-Lease File System)

CLFS is a network file system designed to manage file storage with a claim-lease mechanism, ensuring consistency and cache control.

## How It Works

The server tracks all clients with inode (file or directory on Linux) knowledge and ensures one of the following conditions is met:
1. One client holds an exclusive read/write claim.
2. One or multiple clients hold a shared read-only claim.
3. In shared mode, all operations must be synchronized with the server.

When any access may violate these conditions, the server will revoke the claim, requiring all data to be synced with the server.

Clients can use local caches based on the claims they hold without checking with the server for updates.

The cache includes both metadata and data operations. For example, if a client has a read/write claim for a directory, it can create new files or directories and write them back to the server later.

New files or directories created are automatically claimed as read/write by the creator.

In real-world scenarios, most applications involve widespread reading and relatively narrow writing. For example: setting up a Python virtual environment, checking out a Git repository, or running a batch job. The claim-based system maximizes the cache hit ratio and reduces most round-trips to the server.

In comparison, most network file systems require a round-trip to the server for each file or directory creation or lookup to check if a file has been modified, even with full asynchronous caching enabled. In scenarios where many small files are written or accessed in sequence, CLFS offers a significant performance advantage.

## Prerequisites

- A Linux-based operating system
- Go compiler

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/velda-io/clfs.git
    cd clfs
    ```

2. Build the project:
    ```bash
    make
    ```

3. Run the tests:
    ```bash
    make test
    ```

## Usage

To start the CLFS service:
```bash
sudo ./bin/clfs-server --root /path-to-directory
```

To interact with the file system:
```bash
sudo ./bin/clfs volume mount-point
```

## Planned Features

- Buffer cache invalidation
- Client caching on local disks
- Content-addressable storage: deduplicate files with the same content on the client

## Contributing

Contributions are welcome! Please submit pull requests on GitHub.

## License

This project is licensed under the Apache License. See the [LICENSE](LICENSE) file for details.
