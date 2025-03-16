// MongoDB CDC Replication Program
// This program handles Change Data Capture from MongoDB source cluster (AUTH)
// to target cluster (REPORT) in near real-time, capturing inserts and updates only.

const { MongoClient } = require('mongodb');
const fs = require('fs');
const nodemailer = require('nodemailer');
const winston = require('winston');
const cron = require('node-cron');

// Configuration
const config = {
  source: {
    uri: 'mongodb://user:password@source-cluster-host:27017/',
    dbName: 'AUTH',
    collectionName: 'your_collection'
  },
  target: {
    uri: 'mongodb://user:password@target-cluster-host:27017/',
    dbName: 'REPORT',
    collectionName: 'your_collection'
  },
  checkpointFile: './checkpoint.json',
  checkpointIntervalMs: 30000, // Save checkpoint every 30 seconds
  batchSize: 1000, // Process operations in batches
  alertEmail: {
    enabled: true,
    from: 'alerts@yourcompany.com',
    to: ['dba@yourcompany.com', 'app-support@yourcompany.com'],
    smtpServer: 'smtp.yourcompany.com',
    smtpPort: 587,
    username: 'alerts',
    password: 'your-smtp-password'
  },
  logConfig: {
    level: 'info',
    filename: './cdc-replication.log',
    maxSize: '20m',
    maxFiles: '14d'
  }
};

// Initialize logger
const logger = winston.createLogger({
  level: config.logConfig.level,
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ 
      filename: config.logConfig.filename,
      maxsize: config.logConfig.maxSize,
      maxFiles: config.logConfig.maxFiles
    })
  ]
});

// Email alert service
const emailTransporter = nodemailer.createTransport({
  host: config.alertEmail.smtpServer,
  port: config.alertEmail.smtpPort,
  secure: false,
  auth: {
    user: config.alertEmail.username,
    password: config.alertEmail.password
  }
});

// Send alert
async function sendAlert(subject, message) {
  if (!config.alertEmail.enabled) {
    logger.info(`Alert disabled: ${subject} - ${message}`);
    return;
  }
  
  try {
    await emailTransporter.sendMail({
      from: config.alertEmail.from,
      to: config.alertEmail.to.join(', '),
      subject: `[MongoDB CDC Alert] ${subject}`,
      text: message,
      html: `<p>${message.replace(/\n/g, '<br>')}</p>`
    });
    logger.info(`Alert sent: ${subject}`);
  } catch (err) {
    logger.error(`Failed to send alert: ${err.message}`, { error: err });
  }
}

// Load last checkpoint
function loadCheckpoint() {
  try {
    if (fs.existsSync(config.checkpointFile)) {
      const data = fs.readFileSync(config.checkpointFile, 'utf8');
      return JSON.parse(data);
    }
  } catch (err) {
    logger.error(`Failed to load checkpoint: ${err.message}`, { error: err });
  }
  
  // Default: start from current timestamp if no checkpoint exists
  return { resumeToken: null, timestamp: new Date().toISOString() };
}

// Save checkpoint
function saveCheckpoint(checkpoint) {
  try {
    fs.writeFileSync(config.checkpointFile, JSON.stringify(checkpoint, null, 2));
    logger.debug('Checkpoint saved successfully');
  } catch (err) {
    logger.error(`Failed to save checkpoint: ${err.message}`, { error: err });
    sendAlert('Checkpoint Save Failure', 
      `Failed to save CDC checkpoint. This may cause duplication of records on restart.\n\nError: ${err.message}`);
  }
}

// Connect to MongoDB
async function connectToMongoDB() {
  const sourceClient = new MongoClient(config.source.uri, { useUnifiedTopology: true });
  const targetClient = new MongoClient(config.target.uri, { useUnifiedTopology: true });
  
  try {
    await sourceClient.connect();
    await targetClient.connect();
    
    logger.info('Connected to source and target MongoDB clusters');
    
    return {
      sourceClient,
      sourceDb: sourceClient.db(config.source.dbName),
      sourceCollection: sourceClient.db(config.source.dbName).collection(config.source.collectionName),
      targetClient,
      targetDb: targetClient.db(config.target.dbName),
      targetCollection: targetClient.db(config.target.dbName).collection(config.target.collectionName)
    };
  } catch (err) {
    logger.error(`Failed to connect to MongoDB: ${err.message}`, { error: err });
    if (sourceClient) await sourceClient.close();
    if (targetClient) await targetClient.close();
    sendAlert('MongoDB Connection Failure', 
      `CDC Replication failed to connect to MongoDB clusters.\n\nError: ${err.message}`);
    throw err;
  }
}

// Process a batch of operations
async function processBatch(operations, connections) {
  const { targetCollection } = connections;
  
  if (operations.length === 0) return;
  
  const bulkOps = [];
  let stats = { inserts: 0, updates: 0, errors: 0 };
  
  for (const op of operations) {
    try {
      // We only process inserts and updates (no deletes)
      if (op.operationType === 'insert') {
        bulkOps.push({
          insertOne: {
            document: op.fullDocument
          }
        });
        stats.inserts++;
      } else if (op.operationType === 'update') {
        bulkOps.push({
          updateOne: {
            filter: { _id: op.documentKey._id },
            update: { $set: op.fullDocument },
            upsert: true
          }
        });
        stats.updates++;
      }
      // Ignore delete operations
    } catch (err) {
      stats.errors++;
      logger.error(`Failed to process operation: ${err.message}`, { 
        error: err, 
        operation: op.operationType,
        documentId: op.documentKey ? op.documentKey._id : null
      });
    }
  }
  
  if (bulkOps.length > 0) {
    try {
      const result = await targetCollection.bulkWrite(bulkOps, { ordered: false });
      logger.info(`Processed batch: ${stats.inserts} inserts, ${stats.updates} updates`);
      return stats;
    } catch (err) {
      logger.error(`Bulk write operation failed: ${err.message}`, { error: err });
      sendAlert('Bulk Write Failure', 
        `CDC Replication bulk write operation failed.\n\nError: ${err.message}`);
      throw err;
    }
  }
  
  return stats;
}

// Main CDC replication function
async function startCDCReplication(startTime = null, endTime = null, resumeToken = null) {
  let connections;
  
  try {
    connections = await connectToMongoDB();
    const { sourceCollection } = connections;
    
    // Load checkpoint if not starting from specific parameters
    const checkpoint = (!startTime && !resumeToken) ? loadCheckpoint() : { 
      resumeToken, 
      timestamp: startTime ? new Date(startTime).toISOString() : new Date().toISOString() 
    };
    
    logger.info(`Starting CDC replication${resumeToken ? ' with resume token' : startTime ? ` from ${startTime}` : ' from last checkpoint'}`);
    
    // Set up change stream pipeline to filter only inserts and updates
    const pipeline = [
      { $match: { operationType: { $in: ['insert', 'update'] } } }
    ];
    
    // If specific time window, add time filter
    if (startTime) {
      pipeline[0].$match.clusterTime = { $gte: new Date(startTime) };
      
      if (endTime) {
        pipeline[0].$match.clusterTime.$lte = new Date(endTime);
      }
    }
    
    // Options for change stream
    const options = {
      fullDocument: 'updateLookup',
      batchSize: config.batchSize
    };
    
    // Start from resume token if available
    if (checkpoint.resumeToken) {
      options.resumeAfter = checkpoint.resumeToken;
    } else if (startTime) {
      options.startAtOperationTime = new Date(startTime);
    }
    
    // Create change stream
    const changeStream = sourceCollection.watch(pipeline, options);
    
    // Handle change stream events
    let operations = [];
    let lastCheckpointTime = Date.now();
    let lastResumeToken = null;
    let totalStats = { inserts: 0, updates: 0, errors: 0 };
    
    changeStream.on('change', async (change) => {
      operations.push(change);
      lastResumeToken = change._id;
      
      // Process in batches
      if (operations.length >= config.batchSize) {
        try {
          const stats = await processBatch([...operations], connections);
          operations = [];
          
          // Update stats
          totalStats.inserts += stats.inserts;
          totalStats.updates += stats.updates;
          totalStats.errors += stats.errors;
          
          // Save checkpoint periodically
          if (Date.now() - lastCheckpointTime > config.checkpointIntervalMs) {
            saveCheckpoint({ 
              resumeToken: lastResumeToken, 
              timestamp: new Date().toISOString() 
            });
            lastCheckpointTime = Date.now();
          }
        } catch (err) {
          // Error already logged in processBatch
        }
      }
    });
    
    // Handle errors
    changeStream.on('error', async (error) => {
      logger.error(`Change stream error: ${error.message}`, { error });
      sendAlert('Change Stream Error', 
        `CDC change stream encountered an error.\n\nError: ${error.message}`);
      
      // Clean up and restart
      await changeStream.close();
      await closeConnections(connections);
      
      // Wait before reconnecting
      setTimeout(() => {
        startCDCReplication(null, null, lastResumeToken);
      }, 5000);
    });
    
    // Process remaining operations on close
    changeStream.on('close', async () => {
      if (operations.length > 0) {
        try {
          const stats = await processBatch([...operations], connections);
          totalStats.inserts += stats.inserts;
          totalStats.updates += stats.updates;
          totalStats.errors += stats.errors;
        } catch (err) {
          // Error already logged in processBatch
        }
      }
      
      // Save final checkpoint
      if (lastResumeToken) {
        saveCheckpoint({ 
          resumeToken: lastResumeToken, 
          timestamp: new Date().toISOString() 
        });
      }
      
      logger.info(`Change stream closed. Totals: ${totalStats.inserts} inserts, ${totalStats.updates} updates, ${totalStats.errors} errors`);
    });
    
    // Set up interval to process pending operations
    const processInterval = setInterval(async () => {
      if (operations.length > 0) {
        try {
          const stats = await processBatch([...operations], connections);
          operations = [];
          
          // Update stats
          totalStats.inserts += stats.inserts;
          totalStats.updates += stats.updates;
          totalStats.errors += stats.errors;
          
          // Save checkpoint
          if (lastResumeToken) {
            saveCheckpoint({ 
              resumeToken: lastResumeToken, 
              timestamp: new Date().toISOString() 
            });
            lastCheckpointTime = Date.now();
          }
        } catch (err) {
          // Error already logged in processBatch
        }
      }
    }, 5000);
    
    // Handle process termination
    process.on('SIGINT', async () => {
      logger.info('Received SIGINT. Closing change stream...');
      await changeStream.close();
      clearInterval(processInterval);
      await closeConnections(connections);
      process.exit(0);
    });
    
    process.on('SIGTERM', async () => {
      logger.info('Received SIGTERM. Closing change stream...');
      await changeStream.close();
      clearInterval(processInterval);
      await closeConnections(connections);
      process.exit(0);
    });
    
    return { changeStream, processInterval, connections };
  } catch (err) {
    logger.error(`CDC replication failed to start: ${err.message}`, { error: err });
    sendAlert('CDC Startup Failure', 
      `CDC Replication failed to start.\n\nError: ${err.message}`);
    
    if (connections) {
      await closeConnections(connections);
    }
    
    // Try to restart after delay
    setTimeout(() => {
      startCDCReplication(startTime, endTime, resumeToken);
    }, 60000);
  }
}

// Close MongoDB connections
async function closeConnections(connections) {
  if (!connections) return;
  
  try {
    if (connections.sourceClient) await connections.sourceClient.close();
    if (connections.targetClient) await connections.targetClient.close();
    logger.info('MongoDB connections closed');
  } catch (err) {
    logger.error(`Failed to close MongoDB connections: ${err.message}`, { error: err });
  }
}

// Document comparison function
async function compareDocuments(documentId, startTime, endTime) {
  let connections;
  
  try {
    connections = await connectToMongoDB();
    const { sourceCollection, targetCollection } = connections;
    
    // Get document from source
    const sourceDoc = await sourceCollection.findOne({ _id: documentId });
    
    if (!sourceDoc) {
      return {
        exists: {
          source: false,
          target: false
        },
        document: null,
        differences: 'Document not found in source'
      };
    }
    
    // Get document from target
    const targetDoc = await targetCollection.findOne({ _id: documentId });
    
    if (!targetDoc) {
      return {
        exists: {
          source: true,
          target: false
        },
        document: sourceDoc,
        differences: 'Document not found in target'
      };
    }
    
    // Compare documents
    const differences = findDifferences(sourceDoc, targetDoc);
    
    return {
      exists: {
        source: true,
        target: true
      },
      sourceDocument: sourceDoc,
      targetDocument: targetDoc,
      differences: differences.length > 0 ? differences : null
    };
  } catch (err) {
    logger.error(`Document comparison failed: ${err.message}`, { error: err, documentId });
    throw err;
  } finally {
    if (connections) {
      await closeConnections(connections);
    }
  }
}

// Find differences between documents
function findDifferences(source, target) {
  const differences = [];
  const keys = new Set([...Object.keys(source), ...Object.keys(target)]);
  
  for (const key of keys) {
    // Skip _id
    if (key === '_id') continue;
    
    if (!source.hasOwnProperty(key)) {
      differences.push({ field: key, sourceValue: undefined, targetValue: target[key], type: 'missing-in-source' });
    } else if (!target.hasOwnProperty(key)) {
      differences.push({ field: key, sourceValue: source[key], targetValue: undefined, type: 'missing-in-target' });
    } else if (JSON.stringify(source[key]) !== JSON.stringify(target[key])) {
      differences.push({ field: key, sourceValue: source[key], targetValue: target[key], type: 'value-mismatch' });
    }
  }
  
  return differences;
}

// Time window document comparison
async function compareDocumentsInTimeWindow(startTime, endTime, limit = 100) {
  let connections;
  
  try {
    connections = await connectToMongoDB();
    const { sourceDb, sourceCollection, targetCollection } = connections;
    
    // Find operations in the given time window
    const oplogDb = sourceDb.admin().db('local');
    const oplog = oplogDb.collection('oplog.rs');
    
    const operations = await oplog.find({
      ns: `${config.source.dbName}.${config.source.collectionName}`,
      ts: {
        $gte: new Date(startTime),
        $lte: new Date(endTime)
      },
      op: { $in: ['i', 'u'] } // Only inserts and updates
    }).limit(limit).toArray();
    
    // Get unique document IDs
    const documentIds = [...new Set(operations.map(op => op.o._id || (op.o2 && op.o2._id)))];
    
    // Compare each document
    const results = [];
    for (const id of documentIds) {
      const sourceDoc = await sourceCollection.findOne({ _id: id });
      const targetDoc = await targetCollection.findOne({ _id: id });
      
      const differences = sourceDoc && targetDoc ? findDifferences(sourceDoc, targetDoc) : ['Document missing'];
      
      results.push({
        documentId: id,
        exists: {
          source: !!sourceDoc,
          target: !!targetDoc
        },
        hasDifferences: differences.length > 0,
        differences
      });
    }
    
    return {
      timeWindow: { startTime, endTime },
      totalDocumentsCompared: documentIds.length,
      documentsWithDifferences: results.filter(r => r.hasDifferences).length,
      details: results
    };
  } catch (err) {
    logger.error(`Time window comparison failed: ${err.message}`, { 
      error: err, 
      startTime, 
      endTime 
    });
    throw err;
  } finally {
    if (connections) {
      await closeConnections(connections);
    }
  }
}

// Health check function
async function performHealthCheck() {
  let connections;
  
  try {
    connections = await connectToMongoDB();
    const { sourceClient, targetClient } = connections;
    
    // Check source and target cluster status
    const sourceStatus = await sourceClient.db('admin').command({ serverStatus: 1 });
    const targetStatus = await targetClient.db('admin').command({ serverStatus: 1 });
    
    // Check last checkpoint
    const checkpoint = loadCheckpoint();
    const timeSinceLastCheckpoint = Date.now() - new Date(checkpoint.timestamp).getTime();
    
    // Check if replication is healthy (not too far behind)
    const isHealthy = timeSinceLastCheckpoint < 5 * 60 * 1000; // 5 minutes
    
    // Log health status
    logger.info(`Health check: ${isHealthy ? 'HEALTHY' : 'UNHEALTHY'}, last checkpoint ${timeSinceLastCheckpoint / 1000} seconds ago`);
    
    // Send alert if unhealthy
    if (!isHealthy) {
      sendAlert('CDC Health Check Failed', 
        `CDC Replication health check failed. Replication appears to be ${timeSinceLastCheckpoint / 1000} seconds behind.`);
    }
    
    return {
      healthy: isHealthy,
      sourceStatus: {
        version: sourceStatus.version,
        uptime: sourceStatus.uptime,
        connections: sourceStatus.connections.current
      },
      targetStatus: {
        version: targetStatus.version,
        uptime: targetStatus.uptime,
        connections: targetStatus.connections.current
      },
      replication: {
        lastCheckpoint: checkpoint.timestamp,
        secondsBehind: timeSinceLastCheckpoint / 1000
      }
    };
  } catch (err) {
    logger.error(`Health check failed: ${err.message}`, { error: err });
    
    sendAlert('CDC Health Check Error', 
      `CDC Replication health check encountered an error.\n\nError: ${err.message}`);
    
    return {
      healthy: false,
      error: err.message
    };
  } finally {
    if (connections) {
      await closeConnections(connections);
    }
  }
}

// Schedule health check
cron.schedule('*/5 * * * *', async () => {
  await performHealthCheck();
});

// Main execution
async function main() {
  try {
    logger.info('Starting MongoDB CDC Replication Program');
    
    // Start CDC replication
    await startCDCReplication();
    
    // Start health checks
    await performHealthCheck();
  } catch (err) {
    logger.error(`Program failed: ${err.message}`, { error: err });
    sendAlert('CDC Program Failed', 
      `MongoDB CDC Replication Program failed to start.\n\nError: ${err.message}`);
    process.exit(1);
  }
}

// Expose functions for CLI or API usage
module.exports = {
  startCDCReplication,
  compareDocuments,
  compareDocumentsInTimeWindow,
  performHealthCheck
};

// Run if executed directly
if (require.main === module) {
  main();
}
