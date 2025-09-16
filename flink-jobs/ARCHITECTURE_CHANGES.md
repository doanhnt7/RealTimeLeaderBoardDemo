# Architecture Changes - Flink Fintech POC

## Overview
This document describes the architectural changes made to the Flink Fintech POC to optimize data processing by separating streaming and static data sources.

## Changes Made

### 1. Data Source Separation
- **Before**: All data (transactions, customers, merchants, accounts, customer_sessions) were streamed from Kafka
- **After**: Only streaming data (transactions, customer_sessions) are read from Kafka; static reference data (customers, merchants, accounts) are read from PostgreSQL

### 2. New PostgreSQL Connector
- Created `PostgreSQLConnector.java` to handle reading static reference data from PostgreSQL
- Uses JDBC connections to load data into memory and create DataStreams
- Supports three reference data types:
  - Customers
  - Merchants  
  - Accounts

### 3. Broadcast Streams
- Static reference data is loaded once at startup and converted to broadcast streams
- This allows all processors to access reference data without additional database queries
- Improves performance by reducing database load during streaming processing

### 4. Updated Dependencies
- Added PostgreSQL JDBC driver dependency
- Added Flink JDBC connector dependency
- Removed unused imports and optimized code

## Benefits

### Performance Improvements
- **Reduced Database Load**: Static data is loaded once instead of being queried repeatedly
- **Faster Processing**: Broadcast streams provide instant access to reference data
- **Better Resource Utilization**: Less network traffic between Flink and database

### Scalability
- **Horizontal Scaling**: Streaming processors can scale independently
- **Memory Efficiency**: Reference data is cached in memory across all processors
- **Fault Tolerance**: Broadcast state is automatically replicated across Flink cluster

### Maintainability
- **Clear Separation**: Streaming vs static data processing is clearly separated
- **Easier Debugging**: Reference data issues are isolated from streaming processing
- **Flexible Updates**: Reference data can be updated independently

## Technical Details

### PostgreSQL Connector
```java
// Load customers from PostgreSQL
List<Customer> customers = loadCustomersFromPostgreSQL();
DataStream<Customer> customerStream = env.fromCollection(customers)
    .name("PostgreSQL Customer Source")
    .setParallelism(1);
```

### Broadcast State Usage
```java
// Connect transaction stream with customer broadcast stream
DataStream<EnrichedTransaction> enrichedStream = transactionStream
    .keyBy(Transaction::getFromAccountId)
    .connect(customerStream.broadcast(CUSTOMER_STATE_DESCRIPTOR))
    .process(new CustomerDataEnrichmentProcessor());
```

## Configuration

### PostgreSQL Connection
- **URL**: `jdbc:postgresql://postgres:5432/fintech`
- **Username**: `postgres`
- **Password**: `postgres`

### Required Tables
- `customers` - Customer reference data
- `merchants` - Merchant reference data  
- `accounts` - Account reference data

## Migration Notes

### Breaking Changes
- Customer enrichment now requires account-to-customer mapping
- Some processors may need updates to handle the new data flow
- Database schema must include the required reference tables

### Backward Compatibility
- Transaction and customer_session processing remains unchanged
- Output formats are preserved
- Existing processors continue to work with minor modifications

## Future Enhancements

### Potential Improvements
1. **Incremental Updates**: Support for updating reference data without restarting the job
2. **Caching Strategy**: Implement TTL-based caching for reference data
3. **Monitoring**: Add metrics for reference data freshness and accuracy
4. **Error Handling**: Improve error handling for database connection issues

### Performance Optimizations
1. **Connection Pooling**: Implement connection pooling for database connections
2. **Batch Loading**: Load reference data in batches for better memory management
3. **Compression**: Compress reference data in memory to reduce memory usage

## Testing

### Unit Tests
- Test PostgreSQL connector with mock data
- Test broadcast state functionality
- Test data loading and serialization

### Integration Tests
- Test end-to-end data flow
- Test database connection handling
- Test error scenarios and recovery

### Performance Tests
- Measure memory usage with large reference datasets
- Test processing latency with broadcast streams
- Compare performance with previous architecture

## Conclusion

These changes significantly improve the architecture by:
- Separating concerns between streaming and static data
- Reducing database load and improving performance
- Making the system more scalable and maintainable
- Providing a foundation for future enhancements

The new architecture maintains backward compatibility while providing significant performance improvements and better separation of concerns.
