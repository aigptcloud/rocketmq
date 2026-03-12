# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

This is a Maven-based Java project. All commands should be run from the repository root.

### Build and Test
```bash
# Compile and run unit tests
mvn test

# Run tests for a specific module
mvn test -pl broker

# Run a single test class
mvn test -pl broker -Dtest=BrokerControllerTest

# Run a single test method
mvn test -pl broker -Dtest=BrokerControllerTest#testMethod

# Build distribution packages (creates tar.gz in distribution/target)
mvn -Prelease-all -DskipTests clean install -U

# Compile only (skip tests)
mvn compile -DskipTests

# Install to local repository
mvn clean install -DskipTests
```

### Code Quality
```bash
# Run checkstyle verification
mvn checkstyle:check

# Run checkstyle with specific config
mvn checkstyle:check -Dcheckstyle.config.location=style/rmq_checkstyle.xml

# Generate checkstyle report
mvn checkstyle:checkstyle
```

### Integration Tests
```bash
# Run integration tests (using failsafe plugin)
mvn -Pit-test verify
```

## Architecture Overview

Apache RocketMQ is a distributed messaging and streaming platform. The codebase follows a modular architecture:

### Core Modules

| Module | Purpose | Key Classes |
|--------|---------|-------------|
| **remoting** | Network communication layer built on Netty | `NettyRemotingServer`, `NettyRemotingClient`, `RemotingCommand` |
| **common** | Shared components, protocols, messages | `Message`, `MessageExt`, `MixAll`, `MQVersion` |
| **store** | Message storage and retrieval | `DefaultMessageStore`, `CommitLog`, `ConsumeQueue` |
| **broker** | Message broker server implementation | `BrokerController`, `BrokerStartup`, `SendMessageProcessor`, `PullMessageProcessor` |
| **namesrv** | Service discovery and routing | `NamesrvController`, `NamesrvStartup`, `RouteInfoManager`, `DefaultRequestProcessor` |
| **client** | Producer and consumer implementations | `DefaultMQProducer`, `DefaultMQPushConsumer`, `DefaultMQPullConsumer` |
| **tools** | Admin CLI tools | `MQAdminUtils`, various command implementations |
| **test** | Test utilities and integration tests | `TestUtils`, embedded broker/name server for testing |
| **example** | Sample code for common use cases | `Producer`, `Consumer` in quickstart package |

### Dependency Flow
```
remoting (base network layer)
  ↑
common → client
  ↑       ↑
store ← broker → namesrv
  ↑
tools, acl, filter
```

### Key Architectural Concepts

**Message Storage (store module)**:
- `CommitLog`: Append-only log for all messages
- `ConsumeQueue`: Queue offsets stored separately for fast lookup
- `IndexService`: Message index for query by key
- `MappedFile`: Memory-mapped file for zero-copy I/O

**Broker (broker module)**:
- Processors handle different request types (SendMessageProcessor, PullMessageProcessor, etc.)
- `BrokerController` manages broker lifecycle and components
- Long-polling for push consumers via `PullRequestHoldService`
- Transaction message support via `TransactionalMessageService`

**Name Server (namesrv module)**:
- `RouteInfoManager` holds broker routing metadata in memory
- Lightweight, can be clustered but nodes don't communicate
- Brokers register themselves; clients lookup routes

**Client (client module)**:
- `MQClientInstance` singleton per client group manages connections
- `RebalanceService` handles queue allocation among consumers
- Two consumer models: Push (long-polling) and Pull (explicit fetch)

## Testing

### Test Structure
- Unit tests: `src/test/java` in each module
- Integration tests: `test` module, classes prefixed with `IT`
- Test utilities: `rocketmq-test` module provides `TestUtils`

### Running Tests
```bash
# Unit tests only (excludes IT* classes)
mvn test

# Specific module unit tests
mvn test -pl store

# Integration tests profile
mvn -Pit-test verify

# Skip tests during build
mvn install -DskipTests
```

### Test Dependencies
- JUnit 4.11
- Mockito 2.23.0
- PowerMock 2.0.2
- Google Truth 0.30 (test module)

## Code Conventions

### Checkstyle Rules (`style/rmq_checkstyle.xml`)
- Apache license header required on all files
- No `System.out.println` (use logging)
- No `@author` tags in Javadoc
- No Chinese characters in source files
- Max file length: 3000 lines
- Standard Java naming conventions
- No wildcard imports
- 4-space indentation

### Java Version
- Most modules: Java 8 (source/target 1.8)
- `remoting` and `common` modules: Java 6 compatible (1.6)

### Package Structure
All code under `org.apache.rocketmq.<module>`:
- `org.apache.rocketmq.broker.processor` - Request handlers
- `org.apache.rocketmq.store` - Storage implementation
- `org.apache.rocketmq.client.impl` - Client implementation details
- `org.apache.rocketmq.common.protocol` - Communication protocols

## Running the Servers

After building (`mvn -Prelease-all -DskipTests clean install`):

```bash
# Start Name Server
sh distribution/bin/mqnamesrv

# Start Broker
sh distribution/bin/mqbroker -n localhost:9876

# Run admin commands
sh distribution/bin/mqadmin topicList -n localhost:9876
```

On Windows:
```cmd
distribution\bin\mqnamesrv.cmd
distribution\bin\mqbroker.cmd -n localhost:9876
```

## Version Information
- Current version: 4.6.0
- Maven groupId: org.apache.rocketmq
