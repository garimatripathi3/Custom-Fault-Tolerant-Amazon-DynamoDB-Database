# 🚀 DynamoDB-Inspired Distributed Key-Value Store

![Status](https://img.shields.io/badge/Status-Active-success)
![Python](https://img.shields.io/badge/Python-3.x-blue)

A fault-tolerant distributed key-value store system inspired by Amazon DynamoDB principles. This system provides high availability, resilience against failures, and consistent performance through advanced distributed systems techniques.

## 📋 System Architecture

Our implementation follows a robust distributed architecture with these core components:

### 💻 Client-Server Model
- **Multi-server deployment**: Data distributed across multiple nodes
- **Smart client routing**: Automatic server selection and failover
- **Transparent operation**: Clients interact through simple PUT/GET interface

### 🔄 Data Distribution & Replication
- **Consistent hashing**: Efficient data partitioning with minimal redistribution during node changes
- **Multi-node replication**: Each data item stored on multiple servers
- **Strong consistency**: Write operations propagate to all replicas before confirmation

### ⚡ Fault Tolerance Mechanisms
- **Heartbeat monitoring**: Continuous server health verification
- **Write-Ahead Logging (WAL)**: Operation logging before execution
- **Automated recovery**: System self-heals after node failures
- **Backup snapshots**: Regular data snapshots for disaster recovery

### 🔐 Transaction Support
- **ACID compliance**: Full atomicity, consistency, isolation, durability
- **Two-phase commit**: Ensures transaction integrity across distributed nodes
- **Rollback capability**: Safely handles failed transactions

## 🚀 Getting Started

### Prerequisites
- Python 3.x
- Network connectivity between nodes

### Installation
```bash
# Clone the repository
git clone https://github.com/yourusername/dynamodb-store.git
cd dynamodb-store

# Install dependencies
pip install -r requirements.txt
```

### Running the System

#### 1️⃣ Start the Replica Servers
Open three terminal windows and run:

**Terminal 1:**
```bash
python -m server.server.py --port 5001 --replicas 127.0.0.1:5000
```

**Terminal 2:**
```bash
python -m server.server.py --port 5002 --replicas 127.0.0.1:5000
```

**Terminal 3:**
```bash
python -m server.server.py --port 5000 --replicas 127.0.0.1:5001 127.0.0.1:5002
```

> ⚠️ **Important**: Start the servers in this exact order for proper initialization

#### 2️⃣ Launch the Client
In a new terminal:
```bash
python -m client.client.py
```

## 🔍 Core Features Explained

### Non-Blocking Operations
The system implements asynchronous I/O patterns to ensure that:
- Operations execute concurrently
- Server remains responsive during heavy workloads
- Clients receive immediate acknowledgment while replication happens in background

### Persistent Storage Strategy
- **JSON-based persistence**: All data stored in structured JSON format
- **Incremental updates**: Only changed data written to disk
- **Filesystem syncing**: Forced syncs ensure durability even during power loss

### Consistent Hashing Implementation
Our consistent hashing algorithm:
1. Maps servers and keys to positions on a virtual ring
2. Assigns keys to the next server clockwise on the ring
3. Achieves near-perfect load balancing
4. Minimizes key redistribution when adding/removing servers

### Transaction Processing Flow
1. Client initiates transaction
2. Transaction coordinator (server) prepares all involved nodes
3. Two-phase commit ensures atomic execution
4. Write locks prevent conflicting operations
5. Commit or rollback based on all-node readiness

### Failure Recovery Process
When a server fails:
1. Heartbeat mechanism detects the failure
2. Remaining servers reconfigure the hash ring
3. Recovery process initiated for affected data partitions
4. WAL used to replay missed operations
5. System returns to full redundancy state

## 📊 Performance Benchmarking

The system includes tools to measure key performance indicators:

| Operation | Average Latency | 99th Percentile |
|-----------|-----------------|----------------|
| GET       | 5-15ms          | 30ms           |
| PUT       | 15-30ms         | 60ms           |
| Transaction | 40-80ms       | 120ms          |

Performance varies based on:
- Network conditions
- Data size
- Replication factor
- Server load

## 🔧 Advanced Configuration

The system supports these tunable parameters:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replication_factor` | Number of copies of each data item | 3 |
| `heartbeat_interval` | Seconds between health checks | 5 |
| `wal_sync_interval` | Milliseconds between WAL syncs | 100 |
| `snapshot_interval` | Minutes between backups | 60 |

## 🛠️ Troubleshooting Guide

### Common Issues and Solutions

#### Server Connection Failures
- Verify network connectivity between nodes
- Check firewall settings
- Ensure all servers use correct replica lists

#### Data Inconsistency
- Review transaction logs for partial commits
- Manually reconcile using the `--recover` flag
- Force a full replication with `--sync-all`

#### Performance Degradation
- Check disk I/O with `iostat`
- Monitor network congestion
- Consider reducing the replication factor temporarily

## 🔮 Future Development Roadmap

- [ ] **Enhanced replication strategies**:
  - Implement quorum-based reads/writes
  - Add tunable consistency levels

- [ ] **Advanced consensus protocols**:
  - Replace two-phase commit with Raft/Paxos
  - Improve leader election mechanisms

- [ ] **Self-healing improvements**:
  - Add predictive failure detection
  - Implement automatic capacity scaling

- [ ] **Monitoring dashboard**:
  - Real-time visualization of system health
  - Performance metrics and alerting

## 👥 Contributors

- **Garima Tripathi** - [garimatripathi0778@gmail.com](mailto:garimatripathi0778@gmail.com)
- **Bhavya Kothari** - [bhvyakothari13@gmail.com](mailto:bhvyakothari13@gmail.com)
