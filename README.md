# Ozone Manager Bootstrap Automation

This script automates the process of bootstrapping an unhealthy Ozone Manager follower by downloading a checkpoint from a healthy leader and replacing the local database.

## Overview

The script performs the following steps:

1. **Find healthy leader** (1.0): Discovers the Ozone service and identifies a healthy leader OM
2. **Get configuration** (1.1): Retrieves OM database and Ratis directories from configuration
3. **Check security configuration** (1.2): Validates Kerberos credentials if security is enabled
4. **Check for snapshots** (1.3): Prompts user to confirm no snapshots exist before proceeding
5. **Get OM roles from CLI** (2.0): Retrieves current OM roles and identifies leader/follower status
6. **Check OM leader health** (2.1): Verifies the leader OM is healthy and performs leader switch if needed
7. **List Ratis log files before bootstrapping** (2.2): Captures Ratis log state on both leader and follower
8. **Stop follower** (3.0): Stops the target follower OM via Cloudera Manager
9. **Test checkpoint endpoint** (4.0): Validates the checkpoint download endpoint is accessible
10. **Download checkpoint** (4.1): Downloads the latest consistent checkpoint from the leader OM
11. **Extract checkpoint** (4.2): Extracts the checkpoint to a temporary directory
12. **Backup and replace database** (5.0): Backs up the current database and replaces it with the checkpoint
13. **Backup Ratis logs** (5.1): Backs up existing Ratis logs to prevent conflicts
14. **Start follower** (6.0): Starts the follower OM via Cloudera Manager
15. **Verify status** (7.0): Verifies the OM is healthy and tests leadership transfer
16. **List Ratis log files after bootstrapping** (7.1): Captures Ratis log state after bootstrapping for comparison
17. **Restart OM role** (8.0): Restarts the OM role if it was stopped during the process

## Prerequisites

- Cloudera Manager API access
- SSH access to OM hosts (passwordless SSH recommended)
- Ozone client tools installed on OM hosts
- Proper permissions to stop/start services via CM API
- Python 3.6+ with `requests` package
- **For secured clusters**: Kerberos keytab file and principal
- **SSH connectivity**: Passwordless SSH to CM host and all OM role nodes
- **Sudo access**: Sudo privileges for running privileged commands on remote hosts (not needed when SSH user is root)

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
# Run with dry-run to see what would happen (unsecured cluster, root SSH user)
python ozone_om_bootstrap.py \
  --cm-base-url https://cm:7183 \
  --cluster "Cluster 1" \
  --follower-host om-node-2.example.com \
  --insecure \
  --dry-run

# Run with dry-run (secured cluster with Kerberos, root SSH user)
python ozone_om_bootstrap.py \
  --cm-base-url https://cm:7183 \
  --cluster "Cluster 1" \
  --follower-host om-node-2.example.com \
  --keytab /path/to/keytab \
  --principal user@REALM \
  --insecure \
  --dry-run

# Run with dry-run (non-root SSH user, requires sudo user)
python ozone_om_bootstrap.py \
  --cm-base-url https://cm:7183 \
  --cluster "Cluster 1" \
  --follower-host om-node-2.example.com \
  --ssh-user admin \
  --sudo-user hdfs \
  --insecure \
  --dry-run

# Run actual bootstrap (unsecured cluster, root SSH user)
python ozone_om_bootstrap.py \
  --cm-base-url https://cm:7183 \
  --cluster "Cluster 1" \
  --follower-host om-node-2.example.com \
  --insecure \
  --yes

# Run actual bootstrap (secured cluster with Kerberos, root SSH user)
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
- `--ssh-user`: SSH user for connecting to remote hosts (default: root)
- `--sudo-user`: Sudo user for running privileged commands (not needed when SSH user is root)

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
[1.1] Getting OM configuration...
[>] OM DB directory: /var/lib/hadoop-ozone/om/data5
[>] Ratis directory: /var/lib/hadoop-ozone/om/ratis5
[>] HTTP Kerberos enabled: false
[1.2] Checking security configuration...
[>] Ozone security enabled: false
[1.3] Prompting user to confirm no snapshots exist...

================================================================================
**SNAPSHOT CONFIRMATION REQUIRED**
================================================================================
** STOP! Do not proceed if the cluster enables Ozone Snapshot. Contact Cloudera Storage Engineering team for further instructions if that is the case. **
================================================================================
Before proceeding, please confirm that:
1. You have NO snapshots in the Ozone system
2. You understand that bootstrap operations may impact snapshots
3. You have backed up any important data
================================================================================
Type 'NO' (exactly as shown) to continue with the bootstrap operation.
Any other input will abort the operation.
================================================================================
Enter 'NO' to continue: NO
[>] User confirmed to proceed with bootstrap operation
[2.0] Getting OM roles from CLI...
[>] OM roles output:
LEADER: om-1.prod.example.com
FOLLOWER: om-2.prod.example.com
FOLLOWER: om-3.prod.example.com
[>] Leader OM: om-1.prod.example.com
[>] Target follower: om-2.prod.example.com
[2.1] Checking OM leader health...
[>] Using leader: om-1.prod.example.com
[2.2] Listing last Ratis log files before bootstrapping...
[>] LEADER (om-1.prod.example.com): Last log file: log_inprogress_82838
[>] FOLLOWER (om-2.prod.example.com): Last log file: log_inprogress_73046
[3.0] Stopping follower OM on om-2.prod.example.com...
[>] Stop command initiated: {...}
[>] Follower OM stopped successfully
[4.0] Testing checkpoint endpoint...
[>] Checkpoint endpoint accessible: HTTP/1.1 200 OK
[4.1] Downloading checkpoint from leader OM...
[>] Checkpoint downloaded successfully: -rw-r--r-- 1 hdfs hdfs 1048576 Jan 15 10:30 /tmp/om_bootstrap_1703123456_abc123/om-db-checkpoint.tar
[4.2] Extracting checkpoint...
[>] Checkpoint extracted to /var/lib/hadoop-ozone/om/data5.tmp_1703123456
[5.0] Backing up and replacing OM database...
[>] Database replaced successfully
[5.1] Backing up Ratis logs...
[>] Ratis logs backed up to /backup/om_bootstrap_1703123456/ratisLogs_1703123456
[6.0] Starting follower OM on om-2.prod.example.com...
[>] Start command initiated: {...}
[>] Follower OM started successfully
[7.0] Verifying OM status...
[>] Follower om-2.prod.example.com is healthy
[7.1] Listing last Ratis log files after bootstrapping...
[>] LEADER (om-1.prod.example.com): Last log file: log_inprogress_82838 (unchanged)
[>] FOLLOWER (om-2.prod.example.com): Last log file: log_inprogress_82838 (changed)
[8.0] Restarting OM role if it was stopped during bootstrap...
[>] OM role was stopped during bootstrap, ensuring it's started
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

The script creates backups in the following locations with unique epoch timestamps:

- **Backup Directory**: `/backup/om_bootstrap_{epoch_timestamp}/`
- **OM Database**: `/backup/om_bootstrap_{epoch_timestamp}/om.db.backup.{epoch_timestamp}`
- **Current Database Backup**: `{om_db_dir}/om.db.backup.{epoch_timestamp}`
- **Ratis Logs**: `/backup/om_bootstrap_{epoch_timestamp}/ratisLogs_{epoch_timestamp}/`
- **Original Ratis Logs**: `/backup/om_bootstrap_{epoch_timestamp}/ratisLogs_{epoch_timestamp}/original/`
- **Temporary Directories**: `/tmp/om_bootstrap_{epoch_timestamp}_XXXXXX/`
- **Extract Directory**: `{om_db_dir}.tmp_{epoch_timestamp}`

**Example with epoch timestamp 1703123456:**
- Backup directory: `/backup/om_bootstrap_1703123456/`
- Database backup: `/backup/om_bootstrap_1703123456/om.db.backup.1703123456`
- Ratis logs: `/backup/om_bootstrap_1703123456/ratisLogs_1703123456/`

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

## Snapshot Confirmation

The script includes a critical safety check for snapshots before proceeding with bootstrap operations:

### Why Snapshot Check is Important
Bootstrap operations may affect existing snapshots in the Ozone system. The script prompts users to confirm they have no snapshots before proceeding.

### Snapshot Confirmation Process
1. **User Prompt**: The script displays a prominent warning about snapshot impact
2. **Confirmation Required**: Users must type "NO" exactly to continue
3. **Clear Instructions**: The prompt explains what users need to confirm
4. **Safety First**: Any other input or Ctrl+C aborts the operation

### Example Snapshot Confirmation
```
================================================================================
**SNAPSHOT CONFIRMATION REQUIRED**
================================================================================
** STOP! Do not proceed if the cluster enables Ozone Snapshot. Contact Cloudera Storage Engineering team for further instructions if that is the case. **
================================================================================
Before proceeding, please confirm that:
1. You have NO snapshots in the Ozone system
2. You understand that bootstrap operations may impact snapshots
3. You have backed up any important data
================================================================================
Type 'NO' (exactly as shown) to continue with the bootstrap operation.
Any other input will abort the operation.
================================================================================
Enter 'NO' to continue: 
```

### Best Practices
- **Check for Snapshots**: Manually verify no snapshots exist before running bootstrap
- **Backup Important Data**: Ensure critical data is backed up before proceeding
- **Test in Non-Production**: Always test bootstrap operations in non-production environments first
- **Document Snapshots**: Keep records of any snapshots that might be affected

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
