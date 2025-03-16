# MongoDB CDC Replication Program Deployment Guide

This guide describes how to deploy, configure, and operate the MongoDB CDC (Change Data Capture) Replication Program. This program enables near-real-time replication of inserts and updates (no deletes) from a source MongoDB cluster to a target cluster.

## Prerequisites

- Node.js 14.x or higher
- Access to both source (AUTH) and target (REPORT) MongoDB clusters
- Network connectivity between the deployment server and both MongoDB clusters
- Sufficient permissions to read from source and write to target collections

## Installation

1. Clone or download the repository to your deployment server:

```bash
git clone https://github.com/MinervaDB/mongodb-cdc
cd mongodb-cdc
```

2. Install dependencies:

```bash
npm install
```

3. Create a configuration file:

```bash
cp config.example.json config.json
```

4. Edit `config.json` to update MongoDB connection settings and other configuration parameters.

## Configuration

Edit the `config.json` file to match your environment. Key configuration parameters include:

```json
{
  "source": {
    "uri": "mongodb://user:password@source-cluster-host:27017/",
    "dbName": "AUTH",
    "collectionName": "your_collection"
  },
  "target": {
    "uri": "mongodb://user:password@target-cluster-host:27017/",
    "dbName": "REPORT",
    "collectionName": "your_collection"
  },
  "checkpointFile": "./checkpoint.json",
  "checkpointIntervalMs": 30000,
  "batchSize": 1000,
  "alertEmail": {
    "enabled": true,
    "from": "alerts@yourcompany.com",
    "to": ["dba@yourcompany.com", "support@minervadb.com"],
    "smtpServer": "smtp.yourcompany.com",
    "smtpPort": 587,
    "username": "alerts",
    "password": "your-smtp-password"
  },
  "logConfig": {
    "level": "info",
    "filename": "./cdc-replication.log",
    "maxSize": "20m",
    "maxFiles": "14d"
  }
}
```

### Security Considerations

- Store database credentials securely
- Consider using environment variables for sensitive values
- Ensure MongoDB users have appropriate permissions:
  - Source: `readAnyDatabase` or at minimum read access to the specific collection
  - Target: Write access to the target collection

## Running the Program

### Starting the CDC Replication

```bash
node index.js
```

For production use, consider using a process manager like PM2:

```bash
npm install -g pm2
pm2 start index.js --name mongodb-cdc
pm2 save
```

To run the process as a service:

```bash
pm2 startup
# Follow the instructions to set up the startup script
```

### Command-Line Options

The program accepts several command-line options:

```
--start-time=<ISO-8601-timestamp>  # Start replication from a specific time
--end-time=<ISO-8601-timestamp>    # Stop replication at a specific time (optional)
--compare-id=<document-id>         # Compare a specific document between source and target
--compare-window=<start>,<end>     # Compare documents modified in a time window
--health-check                     # Perform a health check and exit
```

Examples:

```bash
# Start from a specific point in time
node index.js --start-time=2025-03-10T00:00:00Z

# Compare a specific document
node index.js --compare-id=507f1f77bcf86cd799439011

# Compare documents in a time window
node index.js --compare-window=2025-03-10T00:00:00Z,2025-03-10T01:00:00Z
```

## Monitoring and Maintenance

### Logs

Logs are written to the file specified in the configuration (`cdc-replication.log` by default) and also to the console. You can adjust the log level in the configuration.

### Health Checks

The program performs automatic health checks every 5 minutes. To perform a manual health check:

```bash
node index.js --health-check
```

### Checkpoint File

The program maintains a checkpoint file (`checkpoint.json` by default) that stores the last processed operation token. This enables the program to resume from where it left off if restarted.

## Error Handling and Recovery

### Email Alerts

The program sends email alerts on critical errors to the addresses specified in the configuration.

### Restarting from a Specific Point

If you need to replay operations from a specific point in time:

```bash
node index.js --start-time=2025-03-10T14:30:00Z
```

### Document Comparison

To compare a specific document between source and target:

```bash
node index.js --compare-id=507f1f77bcf86cd799439011
```

To compare documents modified in a time window:

```bash
node index.js --compare-window=2025-03-10T14:30:00Z,2025-03-10T15:30:00Z
```

## Performance Considerations

This program is designed to handle high-volume transactions (5+ million records daily, ~600 TPS peak). Key performance parameters:

- `batchSize`: Adjust based on document size and system resources. Default is 1000.
- `checkpointIntervalMs`: How often to save the checkpoint. More frequent saves reduce potential duplication on restart but increase I/O.

## Troubleshooting

Common issues and their solutions:

1. **Connection Errors**:
   - Verify MongoDB URIs and network connectivity
   - Check MongoDB user permissions
   - Verify SSL/TLS settings if used

2. **Replication Lag**:
   - Increase batch size for higher throughput
   - Check target system resources and indexes
   - Monitor source oplog growth rate

3. **Missing Documents**:
   - Use the document comparison tool to identify discrepancies
   - Check for filtering rules that might be excluding documents
   - Verify operations aren't being skipped due to errors

4. **High CPU/Memory Usage**:
   - Reduce batch size
   - Increase server resources
   - Consider distributing the load across multiple instances

## Maintenance Tasks

### Regular Monitoring

- Check logs daily for errors or warnings
- Monitor replication lag
- Verify email alerts are functioning

### Backup and Recovery

- Regularly back up the checkpoint file
- Document recovery procedures for different failure scenarios

### Scaling Considerations

For extremely high-volume deployments:

1. Consider sharding the collection if not already sharded
2. Run multiple CDC instances for different shards
3. Implement a load balancer for distributing write operations to target

## Support and Escalation

For assistance with this program:

1. Contact: `support@minervadb.com`
2. Escalation path: Database Administration → Application Support → Cloud Infrastructure
