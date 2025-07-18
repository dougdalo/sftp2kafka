# sftp2kafka

**sftp2kafka** is a Go-based microservice designed to ingest files from SFTP servers, transform each file line into a JSON message, and automatically publish these messages to a Kafka topic.  
It’s ideal for ETL pipelines, legacy system integrations, and large-scale data ingestion scenarios.

---

## Features

- **Automatic SFTP Connection:** Connects to remote SFTP servers using username and password.
- **CSV/TXT File Support:** Processes delimited files (customizable separator; default is `;`).
- **Line-by-Line to JSON:** Converts every line into a JSON message, with field mapping by header.
- **Dynamic Header Detection:** If `headers.txt` is not found, the service automatically generates field names (`field1`, `field2`, etc.).
- **High Performance Kafka Producer:** Sends messages in batches for optimal throughput.
- **Safe Archiving:** Moves processed files to an archive folder, incrementing filenames to avoid overwrite.
- **Configurable via Environment Variables:** Full setup using a `.env` file.
- **Detailed Logging:** Provides process, error, and performance logs to the terminal.

---

## When to Use

- Automate ingestion of files from legacy or external systems via SFTP into a data lake, data warehouse, or event platform.
- Integrate asynchronous file drops into a Kafka-based streaming data flow.
- Replace fragile shell scripts or manual processes with a resilient, high-performance solution.

---

## Requirements

- Go 1.20 or newer
- Access to Kafka and SFTP servers
- Configured `.env` file (see below)

---

## Example `.env`

```env
# SFTP Settings
SFTP_USER=USER_SFTP
SFTP_PASSWORD=PASSWORD
SFTP_HOST=IP-HOST
SFTP_PORT=22
SFTP_DIR=/DIR_SFTP/
SFTP_FILENAME=.txt
SFTP_HEADERS_FILENAME=headers.txt
SFTP_ARCHIVE_DIR=/DIR_ARCHIVE/archive/

# Kafka Settings
KAFKA_BROKERS=IP-BROKER
KAFKA_TOPIC=topic

# Kafka SASL_SSL Auth
KAFKA_USERNAME=usr-acl
KAFKA_PASSWORD=BHyKqcn7rhNWTzba5MV3ex
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=SCRAM-SHA-256
KAFKA_CA_CERT_PATH=./certs/ca.pem
```


---

## How it Works

1 - Watches the configured SFTP directory for the specified file name.

2 - If the file exists, reads the file and loads headers from headers.txt (or auto-generates field names if missing).

3 - Each line is converted to a JSON object and published to the configured Kafka topic.

4 - After processing, the file is moved to the archive directory with a unique name to prevent overwriting.

---

## Running

1 - Install dependencies
```
go mod download
```

2 - Build:
```
go build -o sftp2kafka

```

3 - Configure your ``.env`` file.

4 - Run
```
./sftp2kafka
```

## License
MIT

