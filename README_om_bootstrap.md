# Ozone Manager Bootstrap Automation

This script automates the process of bootstrapping an unhealthy Ozone Manager follower by downloading a checkpoint from a healthy leader and replacing the local database.

## Overview

The script performs the following steps:

1. **Find a healthy leader**: Discovers the Ozone service and identifies a healthy leader OM
2. **Stop unhealthy follower**: Stops the target follower OM via Cloudera Manager
3. **Locate OM directories**: Identifies OM database and Ratis directories from configuration
4. **Download checkpoint**: Downloads the latest consistent checkpoint from the leader OM
5. **Extract checkpoint**: Extracts the checkpoint to a temporary directory
6. **Backup and replace database**: Backs up the current database and replaces it with the checkpoint
7. **Backup Ratis logs**: Backs up existing Ratis logs to prevent conflicts
8. **Start OM**: Starts the follower OM via Cloudera Manager
9. **Verify status**: Verifies the OM is healthy and tests leadership transfer

## Prerequisites

- Cloudera Manager API access
- SSH access to OM hosts (passwordless SSH recommended)
- Ozone client tools installed on OM hosts
- Proper permissions to stop/start services via CM API
- Python 3.6+ with `requests` package
- **For secured clusters**: Kerberos keytab file and principal

## Installation

```bash
# Install required Python packages
pip install requests

# Make the script executable
chmod +x ozone_om_bootstrap.py
```

## Usage

### Basic Usage

```bash
# Run with dry-run to see what would happen (unsecured cluster)
python ozone_om_bootstrap.py \
  --cm-base-url https://cm:7183 \
  --cluster "Cluster 1" \
  --follower-host om-node-2.example.com \
  --insecure \
  --dry-run

# Run with dry-run (secured cluster with Kerberos)
python ozone_om_bootstrap.py \
  --cm-base-url https://cm:7183 \
  --cluster "Cluster 1" \
  --follower-host om-node-2.example.com \
  --keytab /path/to/keytab \
  --principal user@REALM \
  --insecure \
  --dry-run

# Run actual bootstrap (unsecured cluster)
python ozone_om_bootstrap.py \
  --cm-base-url https://cm:7183 \
  --cluster "Cluster 1" \
  --follower-host om-node-2.example.com \
  --insecure \
  --yes

# Run actual bootstrap (secured cluster with Kerberos)
python ozone_om_bootstrap.py \
  --cm-base-url https://cm:7183 \
  --cluster "Cluster 1" \
  --follower-host om-node-2.example.com \
  --keytab /path/to/keytab \
  --principal user@REALM \
  --insecure \
  --yes
```

### Command Line Options

- `--cm-base-url`: Cloudera Manager base URL (required)
- `--cluster`: Cluster name (required unless --list-clusters is used)
- `--follower-host`: Hostname of the follower OM to bootstrap (required for bootstrap)
- `--username`: CM username (default: admin)
- `--password`: CM password (default: admin)
- `--insecure`: Skip SSL verification
- `--dry-run`: Show what would be done without executing
- `--yes`: Confirm execution (required for non-dry-run)
- `--list-clusters`: List available clusters and exit
- `--keytab`: Path to Kerberos keytab file on CM host (required when Ozone security is enabled)
- `--principal`: Kerberos principal (required when Ozone security is enabled)

### Examples

#### Example 1: Dry Run
```bash
python ozone_om_bootstrap.py \
  --cm-base-url https://cm.example.com:7183 \
  --cluster "Production Cluster" \
  --follower-host om-2.prod.example.com \
  --username admin \
  --password mypassword \
  --insecure \
  --dry-run
```

#### Example 2: Actual Bootstrap
```bash
python ozone_om_bootstrap.py \
  --cm-base-url https://cm.example.com:7183 \
  --cluster "Production Cluster" \
  --follower-host om-2.prod.example.com \
  --username admin \
  --password mypassword \
  --insecure \
  --yes
```

#### Example 3: Secured Cluster with Kerberos
```bash
python ozone_om_bootstrap.py \
  --cm-base-url https://cm.example.com:7183 \
  --cluster "Production Cluster" \
  --follower-host om-2.prod.example.com \
  --keytab /etc/security/keytabs/om.keytab \
  --principal om/om-2.prod.example.com@EXAMPLE.COM \
  --username admin \
  --password mypassword \
  --insecure \
  --dry-run
```

## Output

The script provides detailed output for each step:

```
================================================================================
OZONE MANAGER BOOTSTRAP AUTOMATION
================================================================================
[1.0] Discovering Ozone service information...
[>] Ozone service: OZONE-1
[>] Found 3 OM roles
[1.1] Getting OM roles from ozone admin command...
[>] OM roles output:
LEADER: om-1.prod.example.com
FOLLOWER: om-2.prod.example.com
FOLLOWER: om-3.prod.example.com
[>] Leader OM: om-1.prod.example.com
[>] Follower OMs: ['om-2.prod.example.com', 'om-3.prod.example.com']
[>] Target follower: om-2.prod.example.com
[1.2] Verifying leader OM health...
[>] Using leader: om-1.prod.example.com
[2.0] Stopping follower OM on om-2.prod.example.com...
[>] Stop command initiated: {...}
[>] Follower OM stopped successfully
[3.0] Getting OM configuration...
[>] OM DB directory: /var/lib/hadoop-ozone/om/data5
[>] Ratis directory: /var/lib/hadoop-ozone/om/ratis5
[>] OM HTTP port: 9874
[>] OM HTTPS port: 9875
[4.0] Downloading checkpoint from leader OM...
[>] Checkpoint downloaded successfully: -rw-r--r-- 1 hdfs hdfs 1048576 Jan 15 10:30 /tmp/om_bootstrap_abc123/om-db-checkpoint.tar
[5.0] Extracting checkpoint...
[>] Checkpoint extracted to /var/lib/hadoop-ozone/om/data5.tmp
[6.0] Backing up and replacing OM database...
[>] Database replaced successfully
[7.0] Backing up Ratis logs...
[>] Ratis logs backed up to /backup/ratisLogs
[8.0] Starting follower OM on om-2.prod.example.com...
[>] Start command initiated: {...}
[>] Follower OM started successfully
[9.0] Verifying OM status...
[>] Follower om-2.prod.example.com is healthy
[9.1] Testing leadership transfer...
[>] Testing leadership transfer to node ID: om-2.prod.example.com
[>] Leadership transfer test successful
[>] Leadership transfer test completed, om-2.prod.example.com is follower
================================================================================
BOOTSTRAP PROCESS COMPLETED SUCCESSFULLY
================================================================================
Bootstrap completed successfully!
```

## Error Handling

The script includes comprehensive error handling:

- **Validation**: Validates all required parameters and prerequisites
- **Health checks**: Verifies leader health and performs leader switch if needed
- **Backup creation**: Creates backups before making any changes
- **Rollback capability**: Maintains original files in backup locations
- **Detailed logging**: Provides step-by-step progress and error messages
- **Timeout handling**: Includes timeouts for long-running operations

## Backup Locations

The script creates backups in the following locations:

- **OM Database**: `/backup/om.db.backup.YYYYMMDD_HHMMSS`
- **Ratis Logs**: `/backup/ratisLogs/` and `/backup/ratisLogs/original/`
- **Original Database**: `{om_db_dir}/om.db.backup`

## Troubleshooting

### Common Issues

1. **SSH Connection Failed**
   - Ensure SSH access is configured between the script host and OM hosts
   - Check SSH key authentication or passwordless SSH setup

2. **CM API Access Denied**
   - Verify CM username and password
   - Check CM API permissions for the user

3. **Ozone Commands Not Found**
   - Ensure Ozone client tools are installed on OM hosts
   - Check PATH environment variable

4. **Permission Denied**
   - Ensure the script has proper permissions to execute commands
   - Check file ownership and permissions on OM hosts

### Recovery

If the bootstrap process fails:

1. **Check backups**: Original files are preserved in backup locations
2. **Manual recovery**: Use the backup files to restore the original state
3. **Service restart**: Restart the Ozone service via Cloudera Manager
4. **Log analysis**: Check CM and Ozone logs for detailed error information

## Security Considerations

- Use HTTPS for CM API communication when possible
- Store credentials securely (consider using environment variables)
- Limit SSH access to necessary hosts only
- Review and audit the script before running in production

## Support

For issues or questions:

1. Check the script output for detailed error messages
2. Review Cloudera Manager and Ozone logs
3. Verify all prerequisites are met
4. Test with `--dry-run` first to identify potential issues

## Security Configuration

The script automatically detects if Ozone security is enabled by reading the `ozone.security.enabled` configuration from the Ozone service.

### Unsecured Clusters
For clusters where Ozone security is disabled, no additional authentication is required:
```bash
python ozone_om_bootstrap.py --cm-base-url https://cm:7183 --cluster "Cluster 1" \
  --follower-host om-node-2.example.com --insecure --dry-run
```

### Secured Clusters (Kerberos)
For clusters where Ozone security is enabled, Kerberos authentication is required:
```bash
python ozone_om_bootstrap.py --cm-base-url https://cm:7183 --cluster "Cluster 1" \
  --follower-host om-node-2.example.com --keytab /etc/security/keytabs/om.keytab --principal om/om-node-2.example.com@REALM \
  --insecure --dry-run
```

The script will:
1. Automatically detect if Ozone security is enabled
2. Validate that keytab and principal are provided when security is enabled
3. Verify the keytab file exists on the CM host (where ozone commands run)
4. Test Kerberos authentication before proceeding
5. Automatically add `kinit` commands to all Ozone CLI operations

### Examples
