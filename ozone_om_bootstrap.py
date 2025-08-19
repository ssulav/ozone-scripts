#!/usr/bin/env python3
"""
Ozone Manager Bootstrap Automation

This script automates the process of bootstrapping an unhealthy Ozone Manager follower
by downloading a checkpoint from a healthy leader and replacing the local database.

The script performs the following steps:
1. Find a healthy leader OM and ensure it's healthy
2. Stop an unhealthy follower (one at a time)
3. Locate the Ozone Manager Data Directory
4. Download checkpoint from leader OM
5. Extract checkpoint to temporary directory
6. Backup and replace the OM database
7. Locate and backup Ratis logs
8. Start the Ozone Manager
9. Verify OM status and leadership transfer

Usage:
    python ozone_om_bootstrap.py --cm-base-url https://cm:7183 --cluster "Cluster 1" \
        --follower-host <follower-hostname> --insecure --yes

Requirements:
    - Cloudera Manager API access
    - SSH access to OM hosts
    - Ozone client tools installed
    - Proper permissions to stop/start services
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
import subprocess
import tempfile
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, quote

# Suppress SSL warnings
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", "..", ".."))
(sys.path.insert(0, _REPO_ROOT) if _REPO_ROOT not in sys.path else None)

from common.cm_client import CmClient


class OzoneOMBootstrap:
    """Ozone Manager Bootstrap automation class"""
    
    def __init__(self, cm_client: CmClient, cluster_name: str, follower_host: str, 
                 insecure: bool = False, dry_run: bool = False, keytab: str = None, principal: str = None):
        self.cm_client = cm_client
        self.cluster_name = cluster_name
        self.follower_host = follower_host
        self.insecure = insecure
        self.dry_run = dry_run
        self.keytab = keytab
        self.principal = principal
        
        # Service and role information
        self.ozone_service = None
        self.service_id = None
        self.om_roles = []
        self.leader_host = None
        self.follower_role = None
        
        # Configuration paths
        self.om_db_dir = None
        self.ratis_dir = None
        self.om_http_port = None
        self.om_https_port = None
        self.om_protocol = None
        self.om_port = None
        
        # Security configuration
        self.ozone_security_enabled = False
        self.ozone_http_kerberos_enabled = False
        
        # Backup paths
        self.backup_dir = "/backup"
        self.temp_dir = None
        
        # Generate unique epoch timestamp for all operations
        self.epoch_time = int(time.time())
        self.backup_dir = f"/backup/om_bootstrap_{self.epoch_time}"
        
        # Track if OM role was stopped during bootstrap
        self.om_role_was_stopped = False
        
        # Track Ratis log files for comparison
        self.last_ratis_log_before = None
        self.leader_ratis_log_before = None
        self.follower_ratis_log_before = None
        
    def discover_cluster_info(self) -> bool:
        """Discover Ozone service and OM roles information"""
        print("[1.0] Discovering Ozone service information...")
        
        # First, validate that the cluster exists
        try:
            all_clusters = self.cm_client.list_clusters()
            cluster_exists = False
            available_clusters = []
            
            for cluster in all_clusters:
                cluster_name = cluster.get('name', '')
                available_clusters.append(cluster_name)
                if cluster_name == self.cluster_name:
                    cluster_exists = True
                    break
            
            if not cluster_exists:
                print(f"ERROR: Cluster '{self.cluster_name}' not found in Cloudera Manager", file=sys.stderr)
                print("Available clusters:", file=sys.stderr)
                for cluster_name in available_clusters:
                    print(f"  - '{cluster_name}'", file=sys.stderr)
                print("\nUse the exact cluster name from the list above.", file=sys.stderr)
                return False
                
        except Exception as e:
            print(f"ERROR: Failed to connect to Cloudera Manager: {e}", file=sys.stderr)
            return False
        
        # Find Ozone service
        services = self.cm_client.list_services(self.cluster_name)
        ozone_services = [s for s in services if s.get("type") == "OZONE"]
        
        if not ozone_services:
            print("ERROR: No Ozone service found in cluster", file=sys.stderr)
            print("Available services:", file=sys.stderr)
            for service in services:
                service_name = service.get('name', 'Unknown')
                service_type = service.get('type', 'Unknown')
                print(f"  - {service_name} ({service_type})", file=sys.stderr)
            return False
            
        self.ozone_service = ozone_services[0]["name"]
        print(f"[>] Ozone service: {self.ozone_service}")
        
        # Get OM roles
        roles = self.cm_client.list_roles(self.cluster_name, self.ozone_service)
        om_roles = [r for r in roles if r.get("type") == "OZONE_MANAGER"]
        
        if not om_roles:
            print("ERROR: No Ozone Manager roles found", file=sys.stderr)
            print("Available roles:", file=sys.stderr)
            for role in roles:
                role_name = role.get('name', 'Unknown')
                role_type = role.get('type', 'Unknown')
                print(f"  - {role_name} ({role_type})", file=sys.stderr)
            return False
            
        self.om_roles = om_roles
        print(f"[>] Found {len(om_roles)} OM roles")
        
        # Get service ID
        try:
            service_config = self.cm_client.get_service_config(self.cluster_name, self.ozone_service)
            config_items = service_config.get("items", [])
            for item in config_items:
                if item.get("name") == "ozone.service.id":
                    self.service_id = item.get("value")
                    break
            
            if not self.service_id:
                # Try alternative method - get from service properties
                try:
                    service_props = self.cm_client._request("GET", f"/clusters/{quote(self.cluster_name, safe='')}/services/{quote(self.ozone_service, safe='')}")
                    self.service_id = service_props.get("serviceId")
                except:
                    pass
                
            if not self.service_id:
                print("WARNING: Could not determine service ID from CM config", file=sys.stderr)
                print("Will try to discover service ID from ozone command output", file=sys.stderr)
        except Exception as e:
            print(f"WARNING: Could not get service config: {e}", file=sys.stderr)
            
        return True
    
    def get_om_roles_from_cli(self) -> bool:
        """Get OM roles using ozone admin command"""
        print("[1.3] Getting OM roles from ozone admin command...")
        
        # Use the CM host from cm-base-url
        try:
            command_host = self.cm_client.base_url.split("://")[1].split(":")[0]
            print(f"[>] Using CM host for ozone commands: {command_host}")
        except Exception as e:
            print(f"ERROR: Failed to extract CM host from URL {self.cm_client.base_url}: {e}", file=sys.stderr)
            return False
        
        # Run ozone admin om roles command
        if self.service_id:
            cmd = f"ozone admin om roles -id={self.service_id}"
        else:
            # Try to get service ID first
            print("WARNING: No service ID available, trying to discover it...", file=sys.stderr)
            service_id_cmd = "ozone getconf -confKey ozone.service.id"
            service_id_result = self._run_remote_command(command_host, service_id_cmd)
            
            if service_id_result.returncode == 0 and service_id_result.stdout.strip():
                self.service_id = service_id_result.stdout.strip()
                print(f"[>] Discovered service ID: {self.service_id}")
                cmd = f"ozone admin om roles -id={self.service_id}"
            else:
                print("ERROR: Could not discover service ID, cannot proceed", file=sys.stderr)
                return False
        
        result = self._run_remote_command(command_host, cmd)
        
        if result.returncode != 0:
            print(f"ERROR: Failed to get OM roles: {result.stderr}", file=sys.stderr)
            return False
        
        # Parse roles output
        roles_output = result.stdout
        print(f"[>] OM roles output:\n{roles_output}")
        
        # Parse leader and followers - handle both formats
        # Format 1: LEADER: hostname
        # Format 2: nodeId : LEADER (hostname)
        leader_match = re.search(r'LEADER:\s*([^\s]+)', roles_output)
        if not leader_match:
            # Try alternative format: nodeId : LEADER (hostname)
            leader_match = re.search(r'(\w+)\s*:\s*LEADER\s*\(([^)]+)\)', roles_output)
            if leader_match:
                self.leader_host = leader_match.group(2)  # hostname is in group 2
            else:
                print("WARNING: Could not parse leader from roles output", file=sys.stderr)
        else:
            self.leader_host = leader_match.group(1)
        
        if self.leader_host:
            print(f"[>] Leader OM: {self.leader_host}")
        
        # Parse followers - handle both formats
        follower_matches = re.findall(r'FOLLOWER:\s*([^\s]+)', roles_output)
        if not follower_matches:
            # Try alternative format: nodeId : FOLLOWER (hostname)
            follower_matches = re.findall(r'(\w+)\s*:\s*FOLLOWER\s*\(([^)]+)\)', roles_output)
            if follower_matches:
                # Extract just the hostnames from the matches
                follower_matches = [match[1] for match in follower_matches]
        
        if follower_matches:
            print(f"[>] Follower OMs: {follower_matches}")
            if self.follower_host in follower_matches:
                print(f"[>] Target follower: {self.follower_host}")
                
                # Find the follower role from CM roles
                for role in self.om_roles:
                    host_id = role.get("hostRef", {}).get("hostId")
                    if host_id:
                        host_info = self.cm_client.get_host_by_id(host_id)
                        if host_info.get("hostname") == self.follower_host:
                            self.follower_role = role
                            print(f"[>] Identified follower role: {role.get('name')}")
                            break
                
                if not self.follower_role:
                    print(f"WARNING: Could not identify follower role for {self.follower_host}", file=sys.stderr)
            else:
                print(f"WARNING: Target follower {self.follower_host} not found in follower list", file=sys.stderr)
                return False
        
        return True
    
    def verify_leader_health(self) -> bool:
        """Verify the leader OM is healthy and perform leader switch if needed"""
        print("[1.3] Verifying leader OM health...")
        
        if not self.leader_host:
            print("ERROR: No leader host identified", file=sys.stderr)
            return False
        
        # Check if leader is healthy by running a simple command
        if not self.service_id:
            print("ERROR: Service ID required for health check", file=sys.stderr)
            return False
        health_cmd = f"ozone admin om getserviceroles --service-id={self.service_id}"
        result = self._run_remote_command(self.leader_host, health_cmd)
        
        if result.returncode != 0:
            print(f"WARNING: Leader {self.leader_host} appears unhealthy, attempting leader switch...", file=sys.stderr)
            
            # Find a healthy follower to switch to
            for role in self.om_roles:
                host_id = role.get("hostRef", {}).get("hostId")
                if host_id:
                    host_info = self.cm_client.get_host_by_id(host_id)
                    candidate_host = host_info.get("hostname")
                    
                    if candidate_host and candidate_host != self.leader_host:
                        # Test if this host is healthy
                        test_result = self._run_remote_command(candidate_host, health_cmd)
                        if test_result.returncode == 0:
                            print(f"[>] Switching leadership to healthy follower: {candidate_host}")
                            
                            # Get node ID for transfer
                            node_id_cmd = f"ozone admin om getserviceroles --service-id={self.service_id}"
                            node_result = self._run_remote_command(candidate_host, node_id_cmd)
                            
                            if node_result.returncode == 0:
                                # Parse the output to find the node ID for this candidate
                                output = node_result.stdout
                                node_id = None
                                lines = output.strip().split('\n')
                                for line in lines:
                                    if candidate_host in line and 'FOLLOWER' in line:
                                        # Try different patterns to extract node ID
                                        # Pattern 1: nodeId : FOLLOWER (hostname)
                                        match = re.search(r'(\w+)\s*:\s*FOLLOWER\s*\(([^)]+)\)', line)
                                        if match and candidate_host in match.group(2):
                                            node_id = match.group(1)
                                            break
                                        # Pattern 2: FOLLOWER: hostname (nodeId)
                                        match = re.search(r'FOLLOWER:\s*([^\s]+)\s*\((\w+)\)', line)
                                        if match and candidate_host in match.group(1):
                                            node_id = match.group(2)
                                            break
                                        # Pattern 3: nodeId hostname FOLLOWER
                                        match = re.search(r'(\w+)\s+([^\s]+)\s+FOLLOWER', line)
                                        if match and candidate_host in match.group(2):
                                            node_id = match.group(1)
                                            break
                                
                                if not node_id:
                                    print(f"WARNING: Could not extract node ID for candidate {candidate_host}", file=sys.stderr)
                                    continue
                                transfer_cmd = f"ozone admin om transfer -id={self.service_id} -n {node_id}"
                                transfer_result = self._run_remote_command(candidate_host, transfer_cmd)
                                
                                if transfer_result.returncode == 0:
                                    self.leader_host = candidate_host
                                    print(f"[>] Successfully switched leadership to {candidate_host}")
                                    time.sleep(30)  # Wait for leadership transfer to complete
                                    break
                                else:
                                    print(f"WARNING: Failed to transfer leadership: {transfer_result.stderr}", file=sys.stderr)
        
        print(f"[>] Using leader: {self.leader_host}")
        return True
    
    def stop_follower(self) -> bool:
        """Stop the target follower OM"""
        print(f"[2.0] Stopping follower OM on {self.follower_host}...")
        
        # Find the role name for the follower
        follower_role_name = None
        for role in self.om_roles:
            host_id = role.get("hostRef", {}).get("hostId")
            if host_id:
                host_info = self.cm_client.get_host_by_id(host_id)
                if host_info.get("hostname") == self.follower_host:
                    follower_role_name = role.get("name")
                    self.follower_role = role
                    break
        
        if not follower_role_name:
            print(f"ERROR: Could not find role name for follower {self.follower_host}", file=sys.stderr)
            return False
        
        if self.dry_run:
            print(f"[DRY RUN] Would stop role: {follower_role_name}")
            return True
        
        try:
            result = self.cm_client.role_command(
                self.cluster_name, 
                self.ozone_service, 
                "stop", 
                [follower_role_name]
            )
            print(f"[>] Stop command initiated: {result}")
            
            # Wait for the process to stop by checking for the OzoneManagerStarter process
            print(f"[>] Waiting for OzoneManagerStarter process to stop...")
            max_wait_time = 120  # 2 minutes max wait
            wait_interval = 5    # Check every 5 seconds
            elapsed_time = 0
            
            while elapsed_time < max_wait_time:
                # Check if the OzoneManagerStarter process is still running
                check_cmd = f"pgrep -f 'org.apache.hadoop.ozone.om.OzoneManagerStarter'"
                check_result = self._run_remote_command(self.follower_host, check_cmd)
                
                if check_result.returncode != 0:
                    # Process not found, it has stopped
                    print(f"[>] OzoneManagerStarter process stopped successfully (after {elapsed_time} seconds)")
                    self.om_role_was_stopped = True
                    return True
                
                # Process still running, wait and check again
                time.sleep(wait_interval)
                elapsed_time += wait_interval
                print(f"[>] Process still running, waiting... ({elapsed_time}/{max_wait_time} seconds)")
            
            # If we reach here, the process didn't stop within the timeout
            print(f"WARNING: OzoneManagerStarter process did not stop within {max_wait_time} seconds", file=sys.stderr)
            # print(f"[>] Proceeding anyway, assuming process will stop soon...")
            self.om_role_was_stopped = False
            return False
            
        except Exception as e:
            print(f"ERROR: Failed to stop follower OM: {e}", file=sys.stderr)
            return False
    
    def get_om_configuration(self) -> bool:
        """Get OM configuration including database and ratis directories"""
        print("[1.1] Getting OM configuration...")

        def _extract_value(item: Dict[str, Any]) -> Optional[str]:
            return (
                item.get("value")
                or item.get("effectiveValue")
                or item.get("displayValue")
                or item.get("default")
            )

        try:
            # 1) Read service-level configuration first (for service-level settings)
            svc_cfg = self.cm_client.get_service_config(self.cluster_name, self.ozone_service)
            svc_items = svc_cfg.get("items", [])
            # print(f"[DEBUG] Service config has {len(svc_items)} items")
            
            # Debug: Show all ozone-related configuration items
            ozone_items = [item for item in svc_items if "ozone" in (item.get("name") or "").lower()]
            if ozone_items:
                # print(f"[DEBUG] Found {len(ozone_items)} ozone-related config items in service config:")
                for item in ozone_items[:10]:  # Show first 10
                    name = item.get("name", "")
                    val = _extract_value(item)
                    # print(f"[DEBUG]   {name}: {val}")
                if len(ozone_items) > 10:
                    # print(f"[DEBUG]   ... and {len(ozone_items) - 10} more")
                    pass
            else:
                # print("[DEBUG] No ozone-related config items found in service config")
                pass

            # 2) Read from specific follower role configuration (for DB and Ratis dirs)
            if not self.follower_role:
                print("ERROR: Follower role not identified yet", file=sys.stderr)
                return False
            
            follower_role_name = self.follower_role.get("name")
            if not follower_role_name:
                print("ERROR: Follower role name not found", file=sys.stderr)
                return False
            
            # Get configuration for the specific follower role
            role_cfg = self.cm_client.get_role_config(self.cluster_name, self.ozone_service, follower_role_name, view="FULL")
            role_items = role_cfg.get("items", [])
            # print(f"[DEBUG] Follower role config has {len(role_items)} items")
            
            # If role-specific config is empty, fall back to role config group
            if not role_items:
                print(f"[>] No role-specific config found for {follower_role_name}, using role config group...")
                groups = self.cm_client.list_role_config_groups(self.cluster_name, self.ozone_service)
                om_groups = [g for g in groups if (g.get("roleType") or g.get("roleTypeName") or "").upper() == "OZONE_MANAGER"]
                if not om_groups:
                    print("ERROR: No OZONE_MANAGER role config groups found", file=sys.stderr)
                    return False

                # Prefer the BASE group if present
                base_group = None
                for g in om_groups:
                    gname = g.get("name", "")
                    if gname.endswith("-BASE"):
                        base_group = g
                        break
                if base_group is None:
                    base_group = om_groups[0]

                base_group_name = base_group.get("name")
                # print(f"[DEBUG] Using role config group: {base_group_name}")
                group_cfg = self.cm_client.get_role_config_group_config(self.cluster_name, self.ozone_service, base_group_name, view="FULL")
                role_items = group_cfg.get("items", [])
                # print(f"[DEBUG] Role group config has {len(role_items)} items")
            else:
                print(f"[>] Using role-specific configuration for {follower_role_name}")

            # Parse role-level keys
            ssl_enabled = False
            for item in role_items:
                name = item.get("name") or ""
                val = _extract_value(item)
                if name == "ozone.om.db.dirs" and val:
                    self.om_db_dir = val
                elif name == "ozone.om.ratis.storage.dir" and val:
                    self.ratis_dir = val
                elif name == "ozone.om.http-port" and val:
                    self.om_http_port = val
                    # print(f"[DEBUG] Found ozone.om.http-port in role config: {val}")
                elif name == "ozone.om.https-port" and val:
                    self.om_https_port = val
                    # print(f"[DEBUG] Found ozone.om.https-port in role config: {val}")
                elif name == "ssl_enabled" and val:
                    ssl_enabled = val.lower() == "true"
                    # print(f"[DEBUG] Found ssl_enabled in role config: {val}")
                # Also check for alternative naming patterns
                elif name == "ozone.om.http.port" and val:
                    self.om_http_port = val
                    # print(f"[DEBUG] Found ozone.om.http.port (alternative) in role config: {val}")
                elif name == "ozone.om.https.port" and val:
                    self.om_https_port = val
                    # print(f"[DEBUG] Found ozone.om.https.port (alternative) in role config: {val}")

            # 3) Check service-level configurations
            # print("[DEBUG] Checking service config for additional settings...")
            for item in svc_items:
                name = item.get("name") or ""
                val = _extract_value(item)
                
                # Check for HTTP Kerberos configuration (service-level)
                if name == "ozone.security.http.kerberos.enabled" and val:
                    self.ozone_http_kerberos_enabled = val.lower() == "true"
                    # print(f"[DEBUG] Found ozone.security.http.kerberos.enabled in service config: {val}")
                
                # Fallback for DB and Ratis dirs if not found in role config
                elif name == "ozone.om.db.dirs" and val and not self.om_db_dir:
                    self.om_db_dir = val
                elif name == "ozone.om.ratis.storage.dir" and val and not self.ratis_dir:
                    self.ratis_dir = val

            # 4) Determine protocol based on SSL settings and available ports
            if ssl_enabled and self.om_https_port:
                self.om_protocol = "https"
                self.om_port = self.om_https_port
                # print(f"[DEBUG] SSL enabled, using HTTPS port: {self.om_https_port}")
            elif self.om_http_port:
                self.om_protocol = "http"
                self.om_port = self.om_http_port
                # print(f"[DEBUG] Using HTTP port: {self.om_http_port}")
            else:
                # Fallback to defaults
                self.om_protocol = "http"
                self.om_port = "9874"
                # print(f"[DEBUG] No ports found, using default: {self.om_protocol}://host:{self.om_port}")

            # 5) Validate and log results
            print(f"[>] OM DB directory: {self.om_db_dir}")
            print(f"[>] Ratis directory: {self.ratis_dir}")
            print(f"[>] HTTP Kerberos enabled: {self.ozone_http_kerberos_enabled}")
            print(f"[>] Using protocol: {self.om_protocol}")
            print(f"[>] Using port: {self.om_port}")

            # Minimal required: OM DB dir and Ratis dir
            if not self.om_db_dir:
                print("ERROR: Could not determine 'ozone.om.db.dirs' from global configs", file=sys.stderr)
                return False
            if not self.ratis_dir:
                print("WARNING: Could not determine 'ozone.om.ratis.storage.dir' from global configs", file=sys.stderr)

            return True

        except Exception as e:
            print(f"ERROR: Failed to get OM configuration: {e}", file=sys.stderr)
            return False
    
    def download_checkpoint(self) -> bool:
        """Download checkpoint from leader OM"""
        print("[4.1] Downloading checkpoint from leader OM...")
        
        if not self.leader_host:
            print("ERROR: No leader host available", file=sys.stderr)
            return False
        
        # Use the protocol and port determined from configuration
        protocol = getattr(self, 'om_protocol', 'http')
        port = getattr(self, 'om_port', '9874')
        
        # Create temporary directory on remote host for download with epoch timestamp
        temp_dir_cmd = f"mktemp -d /tmp/om_bootstrap_{self.epoch_time}_XXXXXX"
        temp_dir_result = self._run_remote_command(self.follower_host, temp_dir_cmd)
        
        if temp_dir_result.returncode != 0:
            print(f"ERROR: Failed to create temporary directory on remote host: {temp_dir_result.stderr}", file=sys.stderr)
            return False
        
        self.temp_dir = temp_dir_result.stdout.strip()
        checkpoint_file = f"{self.temp_dir}/om-db-checkpoint.tar"
        
        # print(f"[DEBUG] Created temporary directory on remote host: {self.temp_dir}")
        
        # Build curl command
        curl_cmd = ["curl", "-k", "-s"]  # Use silent mode instead of verbose
        
        # Add Kerberos authentication if enabled
        if self.ozone_http_kerberos_enabled:
            if not self.keytab or not self.principal:
                print("ERROR: HTTP Kerberos is enabled but --keytab and --principal are required", file=sys.stderr)
                return False
            
            # Add negotiate authentication
            curl_cmd.extend(["--negotiate", "-u", ":"])
            print(f"[>] Using Kerberos authentication for HTTP request")
        
        curl_cmd.extend([
            f"{protocol}://{self.leader_host}:{port}/dbCheckpoint?flushBeforeCheckpoint=true",
            "-o", checkpoint_file
        ])
        
        if self.dry_run:
            print(f"[DRY RUN] Would run: {' '.join(curl_cmd)}")
            return True
        
        # Run curl command on follower host
        result = self._run_remote_command(self.follower_host, ' '.join(curl_cmd))
        
        if result.returncode != 0:
            print(f"ERROR: Failed to download checkpoint: {result.stderr}", file=sys.stderr)
            return False
        
        # Verify file was downloaded and check its content
        ls_cmd = f"ls -la {checkpoint_file}"
        ls_result = self._run_remote_command(self.follower_host, ls_cmd)
        
        if ls_result.returncode != 0:
            print(f"ERROR: Checkpoint file not found after download", file=sys.stderr)
            return False
        
        print(f"[>] Checkpoint downloaded: {ls_result.stdout.strip()}")
        
        # Check file type and content
        file_cmd = f"file {checkpoint_file}"
        file_result = self._run_remote_command(self.follower_host, file_cmd)
        
        if file_result.returncode == 0:
            print(f"[>] File type: {file_result.stdout.strip()}")
        
        # Check first few bytes to see if it's a tar file
        head_cmd = f"head -c 50 {checkpoint_file} | hexdump -C"
        head_result = self._run_remote_command(self.follower_host, head_cmd)
        
        if head_result.returncode == 0:
            print(f"[>] File header (first 50 bytes):")
            print(head_result.stdout)
        
        # Check if the file contains error messages (common with HTTP errors)
        grep_cmd = f"grep -i 'error\\|exception\\|failed' {checkpoint_file} | head -5"
        grep_result = self._run_remote_command(self.follower_host, grep_cmd)
        
        if grep_result.returncode == 0 and grep_result.stdout.strip():
            print(f"WARNING: File contains error messages: {grep_result.stdout.strip()}", file=sys.stderr)
        
        # Test if it's a valid tar file
        tar_test_cmd = f"tar -tf {checkpoint_file} > /dev/null 2>&1"
        tar_test_result = self._run_remote_command(self.follower_host, tar_test_cmd)
        
        if tar_test_result.returncode != 0:
            print(f"ERROR: Downloaded file is not a valid tar archive", file=sys.stderr)
            print(f"ERROR: This suggests the checkpoint download failed or returned an error response", file=sys.stderr)
            
            return False
        
        print(f"[>] Checkpoint file is a valid tar archive")
        return True
    
    def list_last_ratis_log(self, stage: str = "current", host: str = None) -> bool:
        """List the last Ratis log file from the Ratis log directory"""
        if not host:
            host = self.follower_host
            
        host_label = "LEADER" if host == self.leader_host else "FOLLOWER"
        print(f"[{stage.upper()}] Listing last Ratis log file on {host_label} ({host})...")
        
        if not self.ratis_dir:
            print("ERROR: Ratis directory not configured", file=sys.stderr)
            return False
        
        # Find the last log file in the Ratis directory
        # Handle the structure: /var/lib/hadoop-ozone/om/ratis/{uuid}/current/
        # Look for log_* files (including log_inprogress_*)
        find_cmd = f"find {self.ratis_dir} -name 'log_*' -type f -printf '%T@ %p\n' | sort -n | tail -1"
        find_result = self._run_remote_command(host, find_cmd)
        
        if find_result.returncode != 0 or not find_result.stdout.strip():
            print(f"WARNING: No log_* files found in Ratis directory on {host}: {self.ratis_dir}", file=sys.stderr)
            return False
        
        # Extract the file path (remove timestamp)
        last_log_file = find_result.stdout.strip().split(' ', 1)[1]
        
        # Get file details
        ls_cmd = f"ls -la {last_log_file}"
        ls_result = self._run_remote_command(host, ls_cmd)
        
        if ls_result.returncode == 0:
            print(f"[>] {host_label} Ratis log file: {ls_result.stdout.strip()}")
            
            # Store the file path for comparison
            if stage.lower() == "before":
                if host == self.leader_host:
                    self.leader_ratis_log_before = last_log_file
                else:
                    self.follower_ratis_log_before = last_log_file
            elif stage.lower() == "after":
                # Compare with the before log file
                before_log = None
                if host == self.leader_host:
                    before_log = self.leader_ratis_log_before
                else:
                    before_log = self.follower_ratis_log_before
                
                if before_log and last_log_file != before_log:
                    print(f"[>] {host_label} Ratis log file changed during bootstrap:")
                    print(f"    Before: {before_log}")
                    print(f"    After:  {last_log_file}")
                elif before_log:
                    print(f"[>] {host_label} Ratis log file unchanged during bootstrap")
            
            return True
        else:
                    print(f"ERROR: Failed to get file details on {host}: {ls_result.stderr}", file=sys.stderr)
        return False
    
    def check_snapshots_and_prompt(self) -> bool:
        """Prompt user to confirm there are no snapshots in the system"""
        print("\n" + "="*80)
        print("**SNAPSHOT CONFIRMATION REQUIRED**")
        print("="*80)
        print("**WARNING: Bootstrap operation may affect existing snapshots**")
        print("="*80)
        print("Before proceeding, please confirm that:")
        print("1. You have NO snapshots in the Ozone system")
        print("2. You understand that bootstrap operations may impact snapshots")
        print("3. You have backed up any important data")
        print("="*80)
        print("Type 'NO' (exactly as shown) to continue with the bootstrap operation.")
        print("Any other input will abort the operation.")
        print("="*80)
        
        try:
            user_input = input("Enter 'NO' to continue: ").strip()
            if user_input == "NO":
                print("[>] User confirmed to proceed with bootstrap operation")
                return True
            else:
                print("[>] User aborted the operation")
                return False
        except KeyboardInterrupt:
            print("\n[>] Operation aborted by user")
            return False
    
    def test_checkpoint_endpoint(self) -> bool:
        """Test the checkpoint endpoint to ensure it's working"""
        print("[4.0] Testing checkpoint endpoint...")
        
        if not self.leader_host or not self.om_protocol or not self.om_port:
            print("ERROR: Missing leader host or OM configuration", file=sys.stderr)
            return False
        
        # Test the endpoint with a HEAD request first
        test_cmd = ["curl", "-k", "-I", "-s"]  # Added -s for silent mode
        
        # Add Kerberos authentication if enabled
        if self.ozone_http_kerberos_enabled:
            if not self.keytab or not self.principal:
                print("ERROR: HTTP Kerberos is enabled but --keytab and --principal are required", file=sys.stderr)
                return False
            
            # Add negotiate authentication
            test_cmd.extend(["--negotiate", "-u", ":"])
            print(f"[>] Using Kerberos authentication for endpoint test")
        
        test_cmd.append(f"{self.om_protocol}://{self.leader_host}:{self.om_port}/dbCheckpoint")
        test_cmd_str = ' '.join(test_cmd)
        test_result = self._run_remote_command(self.follower_host, test_cmd_str)
        
        if test_result.returncode != 0:
            print(f"ERROR: Checkpoint endpoint test failed: {test_result.stderr}", file=sys.stderr)
            return False
        
        # Extract just the HTTP status line
        lines = test_result.stdout.strip().split('\n')
        status_line = None
        for line in lines:
            if line.startswith('HTTP/'):
                status_line = line.strip()
                break
        
        if status_line:
            print(f"[>] Checkpoint endpoint test successful: {status_line}")
        else:
            print(f"[>] Checkpoint endpoint test successful")
        
        return True
    
    def extract_checkpoint(self) -> bool:
        """Extract checkpoint to temporary directory"""
        print("[5.0] Extracting checkpoint...")
        
        if not self.om_db_dir or not self.temp_dir:
            print("ERROR: Missing OM DB directory or temp directory", file=sys.stderr)
            return False
        
        checkpoint_file = f"{self.temp_dir}/om-db-checkpoint.tar"
        extract_dir = f"{self.om_db_dir}.tmp_{self.epoch_time}"
        
        # Create temporary directory
        mkdir_cmd = f"mkdir -p {extract_dir}"
        mkdir_result = self._run_remote_command(self.follower_host, mkdir_cmd)
        
        if mkdir_result.returncode != 0:
            print(f"ERROR: Failed to create extract directory: {mkdir_result.stderr}", file=sys.stderr)
            return False
        
        # Extract checkpoint
        tar_cmd = f"tar -xvf {checkpoint_file} -C {extract_dir}"
        
        if self.dry_run:
            print(f"[DRY RUN] Would run: {tar_cmd}")
            return True
        
        tar_result = self._run_remote_command(self.follower_host, tar_cmd)
        
        if tar_result.returncode != 0:
            print(f"ERROR: Failed to extract checkpoint: {tar_result.stderr}", file=sys.stderr)
            print(f"ERROR: This usually means the downloaded file is not a valid tar archive", file=sys.stderr)
            print(f"ERROR: Please check the download step output above for more details", file=sys.stderr)
            return False
        
        print(f"[>] Checkpoint extracted to {extract_dir}")
        return True
    
    def backup_and_replace_database(self) -> bool:
        """Backup current database and replace with checkpoint"""
        print("[6.0] Backing up and replacing OM database...")
        
        if not self.om_db_dir:
            print("ERROR: OM DB directory not configured", file=sys.stderr)
            return False
        
        # Create backup directory
        backup_cmd = f"mkdir -p {self.backup_dir}"
        backup_result = self._run_remote_command(self.follower_host, backup_cmd)
        
        if backup_result.returncode != 0:
            print(f"ERROR: Failed to create backup directory: {backup_result.stderr}", file=sys.stderr)
            return False
        
        # Backup current database
        current_db = f"{self.om_db_dir}/om.db"
        backup_db = f"{self.backup_dir}/om.db.backup.{self.epoch_time}"
        backup_current_cmd = f"cp -r {current_db} {backup_db}"
        
        if self.dry_run:
            print(f"[DRY RUN] Would run: {backup_current_cmd}")
        else:
            backup_current_result = self._run_remote_command(self.follower_host, backup_current_cmd)
            if backup_current_result.returncode != 0:
                print(f"ERROR: Failed to backup current database: {backup_current_result.stderr}", file=sys.stderr)
                return False
        
        # Move current database to backup with epoch timestamp
        move_current_cmd = f"mv {current_db} {current_db}.backup.{self.epoch_time}"
        
        if self.dry_run:
            print(f"[DRY RUN] Would run: {move_current_cmd}")
        else:
            move_current_result = self._run_remote_command(self.follower_host, move_current_cmd)
            if move_current_result.returncode != 0:
                print(f"ERROR: Failed to move current database: {move_current_result.stderr}", file=sys.stderr)
                return False
        
        # Move new checkpoint into place
        new_db = f"{self.om_db_dir}.tmp_{self.epoch_time}"
        move_new_cmd = f"mv {new_db} {current_db}"
        
        if self.dry_run:
            print(f"[DRY RUN] Would run: {move_new_cmd}")
        else:
            move_new_result = self._run_remote_command(self.follower_host, move_new_cmd)
            if move_new_result.returncode != 0:
                print(f"ERROR: Failed to move new database: {move_new_result.stderr}", file=sys.stderr)
                return False
        
        # Set correct ownership
        chown_cmd = f"chown -R hdfs:hdfs {current_db}"
        
        if self.dry_run:
            print(f"[DRY RUN] Would run: {chown_cmd}")
        else:
            chown_result = self._run_remote_command(self.follower_host, chown_cmd)
            if chown_result.returncode != 0:
                print(f"WARNING: Failed to set ownership: {chown_result.stderr}", file=sys.stderr)
        
        print(f"[>] Database replaced successfully")
        return True
    
    def backup_ratis_logs(self) -> bool:
        """Backup Ratis logs"""
        print("[7.0] Backing up Ratis logs...")
        
        if not self.ratis_dir:
            print("WARNING: Ratis directory not configured, skipping Ratis log backup", file=sys.stderr)
            return True
        
        # Find the Raft group directory
        find_group_cmd = f"find {self.ratis_dir} -name 'current' -type d | head -1"
        find_result = self._run_remote_command(self.follower_host, find_group_cmd)
        
        if find_result.returncode != 0 or not find_result.stdout.strip():
            print(f"WARNING: Could not find Ratis current directory", file=sys.stderr)
            return True
        
        current_dir = find_result.stdout.strip()
        group_dir = os.path.dirname(current_dir)
        
        # Create backup directories with epoch timestamp
        backup_ratis_cmd = f"mkdir -p {self.backup_dir}/ratisLogs_{self.epoch_time} {self.backup_dir}/ratisLogs_{self.epoch_time}/original"
        backup_ratis_result = self._run_remote_command(self.follower_host, backup_ratis_cmd)
        
        if backup_ratis_result.returncode != 0:
            print(f"ERROR: Failed to create Ratis backup directories: {backup_ratis_result.stderr}", file=sys.stderr)
            return False
        
        # Backup current Raft logs
        backup_logs_cmd = f"cp {current_dir}/log* {self.backup_dir}/ratisLogs_{self.epoch_time}/ 2>/dev/null || true"
        
        if self.dry_run:
            print(f"[DRY RUN] Would run: {backup_logs_cmd}")
        else:
            backup_logs_result = self._run_remote_command(self.follower_host, backup_logs_cmd)
            if backup_logs_result.returncode != 0:
                print(f"WARNING: Failed to backup Ratis logs: {backup_logs_result.stderr}", file=sys.stderr)
        
        # Move original logs to safe location
        move_logs_cmd = f"mv {current_dir}/log* {self.backup_dir}/ratisLogs_{self.epoch_time}/original/ 2>/dev/null || true"
        
        if self.dry_run:
            print(f"[DRY RUN] Would run: {move_logs_cmd}")
        else:
            move_logs_result = self._run_remote_command(self.follower_host, move_logs_cmd)
            if move_logs_result.returncode != 0:
                print(f"WARNING: Failed to move Ratis logs: {move_logs_result.stderr}", file=sys.stderr)
        
        print(f"[>] Ratis logs backed up to {self.backup_dir}/ratisLogs_{self.epoch_time}")
        return True
    
    def start_follower(self) -> bool:
        """Start the follower OM"""
        print(f"[8.0] Starting follower OM on {self.follower_host}...")
        
        if not self.follower_role:
            print("ERROR: Follower role not identified", file=sys.stderr)
            return False
        
        role_name = self.follower_role.get("name")
        
        if self.dry_run:
            print(f"[DRY RUN] Would start role: {role_name}")
            return True
        
        try:
            result = self.cm_client.role_command(
                self.cluster_name, 
                self.ozone_service, 
                "start", 
                [role_name]
            )
            print(f"[>] Start command initiated: {result}")
            
            # Wait for the process to start by checking for the OzoneManagerStarter process
            print(f"[>] Waiting for OzoneManagerStarter process to start...")
            max_wait_time = 180  # 3 minutes max wait
            wait_interval = 10   # Check every 10 seconds
            elapsed_time = 0
            
            while elapsed_time < max_wait_time:
                # Check if the OzoneManagerStarter process is running
                check_cmd = f"pgrep -f 'org.apache.hadoop.ozone.om.OzoneManagerStarter'"
                check_result = self._run_remote_command(self.follower_host, check_cmd)
                
                if check_result.returncode == 0:
                    # Process found, it has started
                    print(f"[>] OzoneManagerStarter process started successfully (after {elapsed_time} seconds)")
                    self.om_role_was_stopped = False
                    return True
                
                # Process not running yet, wait and check again
                time.sleep(wait_interval)
                elapsed_time += wait_interval
                print(f"[>] Process not running yet, waiting... ({elapsed_time}/{max_wait_time} seconds)")
            
            # If we reach here, the process didn't start within the timeout
            print(f"ERROR: OzoneManagerStarter process did not start within {max_wait_time} seconds", file=sys.stderr)
            return False
            
        except Exception as e:
            print(f"ERROR: Failed to start follower OM: {e}", file=sys.stderr)
            return False
    
    def verify_om_status(self) -> bool:
        """Verify OM status and test leadership transfer"""
        print("[9.0] Verifying OM status...")
        
        # Wait a bit more for OM to fully join the cluster
        time.sleep(30)
        
        # Get updated roles
        if not self._get_om_roles_from_cli():
            print("ERROR: Failed to get updated OM roles", file=sys.stderr)
            return False
        
        # Check if our follower is now healthy
        if not self.service_id:
            print("ERROR: Service ID required for health check", file=sys.stderr)
            return False
        health_cmd = f"ozone admin om getserviceroles --service-id={self.service_id}"
        result = self._run_remote_command(self.follower_host, health_cmd)
        
        if result.returncode != 0:
            print(f"WARNING: Follower {self.follower_host} still appears unhealthy", file=sys.stderr)
            return False
        
        print(f"[>] Follower {self.follower_host} is healthy")
        
        # Test leadership transfer
        print("[9.1] Testing leadership transfer...")
        
        # Get node ID for the bootstrapped follower
        node_id_cmd = f"ozone admin om getserviceroles --service-id={self.service_id}"
        node_result = self._run_remote_command(self.follower_host, node_id_cmd)
        
        if node_result.returncode == 0:
            # Parse the output to find the node ID for our follower
            output = node_result.stdout
            # print(f"[DEBUG] getserviceroles output:\n{output}")
            
            # Look for our follower host in the output and extract its node ID
            node_id = None
            lines = output.strip().split('\n')
            for line in lines:
                if self.follower_host in line and 'FOLLOWER' in line:
                    # Try different patterns to extract node ID
                    # Pattern 1: nodeId : FOLLOWER (hostname)
                    match = re.search(r'(\w+)\s*:\s*FOLLOWER\s*\(([^)]+)\)', line)
                    if match and self.follower_host in match.group(2):
                        node_id = match.group(1)
                        break
                    # Pattern 2: FOLLOWER: hostname (nodeId)
                    match = re.search(r'FOLLOWER:\s*([^\s]+)\s*\((\w+)\)', line)
                    if match and self.follower_host in match.group(1):
                        node_id = match.group(2)
                        break
                    # Pattern 3: nodeId hostname FOLLOWER
                    match = re.search(r'(\w+)\s+([^\s]+)\s+FOLLOWER', line)
                    if match and self.follower_host in match.group(2):
                        node_id = match.group(1)
                        break
            
            if node_id:
                print(f"[>] Testing leadership transfer to node ID: {node_id}")
            else:
                print(f"ERROR: Could not extract node ID for follower {self.follower_host}", file=sys.stderr)
                print(f"Available lines: {lines}", file=sys.stderr)
                return False
        else:
            print(f"ERROR: Failed to get service roles: {node_result.stderr}", file=sys.stderr)
            return False
        
        # Now proceed with the transfer
        transfer_cmd = f"ozone admin om transfer -id={self.service_id} -n {node_id}"
        transfer_result = self._run_remote_command(self.follower_host, transfer_cmd)
        
        if transfer_result.returncode == 0:
            print(f"[>] Leadership transfer test successful")
            
            # Wait for transfer to complete
            time.sleep(30)
            
            # Verify the transfer
            if self._get_om_roles_from_cli():
                if self.leader_host == self.follower_host:
                    print(f"[>] Leadership transfer confirmed: {self.follower_host} is now leader")
                else:
                    print(f"[>] Leadership transfer test completed, {self.follower_host} is follower")
            
            return True
        else:
            print(f"WARNING: Leadership transfer test failed: {transfer_result.stderr}", file=sys.stderr)
            return False
    
    def cleanup(self):
        """Clean up temporary files"""
        if self.temp_dir:
            # Clean up remote temporary directory
            cleanup_cmd = f"rm -rf {self.temp_dir}"
            cleanup_result = self._run_remote_command(self.follower_host, cleanup_cmd)
            
            if cleanup_result.returncode == 0:
                print(f"[>] Cleaned up remote temporary directory: {self.temp_dir}")
            else:
                print(f"WARNING: Failed to clean up remote temporary directory {self.temp_dir}: {cleanup_result.stderr}")
    
    def _run_remote_command(self, host: str, command: str) -> subprocess.CompletedProcess:
        """Run a command on a remote host via SSH"""
        
        # If Ozone security is enabled and this is an ozone command, add kinit
        if self.ozone_security_enabled and ('ozone' in command.lower() or 'admin' in command.lower()):
            if self.keytab and self.principal:
                kinit_cmd = f"kinit -kt {self.keytab} {self.principal} && {command}"
            else:
                # If no keytab/principal but security is enabled, just use kinit
                kinit_cmd = f"kinit && {command}"
        else:
            kinit_cmd = command
        
        ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", host, kinit_cmd]
        
        try:
            result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                timeout=300
            )
            return result
        except subprocess.TimeoutExpired:
            return subprocess.CompletedProcess(ssh_cmd, -1, "", "Command timed out")
        except Exception as e:
            return subprocess.CompletedProcess(ssh_cmd, -1, "", str(e))
    
    def _get_om_roles_from_cli(self) -> bool:
        """Internal method to get OM roles from CLI"""
        # Use the CM host from cm-base-url
        try:
            command_host = self.cm_client.base_url.split("://")[1].split(":")[0]
            print(f"[>] Using CM host for ozone commands: {command_host}")
        except Exception as e:
            print(f"ERROR: Failed to extract CM host from URL {self.cm_client.base_url}: {e}", file=sys.stderr)
            return False
        
        # Run ozone admin om roles command
        cmd = f"ozone admin om roles -id={self.service_id}" if self.service_id else "ozone admin om roles"
        result = self._run_remote_command(command_host, cmd)
        
        if result.returncode != 0:
            return False
        
        # Parse roles output
        roles_output = result.stdout
        
        # Parse leader and followers - handle both formats
        # Format 1: LEADER: hostname
        # Format 2: nodeId : LEADER (hostname)
        leader_match = re.search(r'LEADER:\s*([^\s]+)', roles_output)
        if not leader_match:
            # Try alternative format: nodeId : LEADER (hostname)
            leader_match = re.search(r'(\w+)\s*:\s*LEADER\s*\(([^)]+)\)', roles_output)
            if leader_match:
                self.leader_host = leader_match.group(2)  # hostname is in group 2
        else:
            self.leader_host = leader_match.group(1)
        
        return True
    
    def check_security_configuration(self) -> bool:
        """Check if Ozone security is enabled and validate Kerberos credentials if needed"""
        print("[1.2] Checking Ozone security configuration...")
        
        if not self.ozone_service:
            print("ERROR: Ozone service not discovered yet", file=sys.stderr)
            return False
        
        try:
            # Get Ozone service configuration
            service_config = self.cm_client.get_service_config(self.cluster_name, self.ozone_service)
            config_items = service_config.get("items", [])
            
            # Look for ozone.security.enabled
            for item in config_items:
                if item.get("name") == "ozone.security.enabled":
                    security_enabled = item.get("value", "false").lower() == "true"
                    self.ozone_security_enabled = security_enabled
                    print(f"[>] Ozone security enabled: {security_enabled}")
                    break
            
            if self.ozone_security_enabled:
                print("[>] Kerberos authentication required for Ozone operations")
                
                # Check if keytab and principal are provided
                if not self.keytab or not self.principal:
                    print("ERROR: Ozone security is enabled but Kerberos credentials are missing", file=sys.stderr)
                    print("Please provide --keytab and --principal options", file=sys.stderr)
                    return False
            
            # Check HTTP Kerberos configuration (this is separate from ozone.security.enabled)
            if self.ozone_http_kerberos_enabled:
                print("[>] HTTP Kerberos authentication required for checkpoint download")
                
                # Check if keytab and principal are provided
                if not self.keytab or not self.principal:
                    print("ERROR: HTTP Kerberos is enabled but Kerberos credentials are missing", file=sys.stderr)
                    print("Please provide --keytab and --principal options", file=sys.stderr)
                    return False
                
                # Use CM host for keytab validation since ozone commands run there
                try:
                    cm_host = self.cm_client.base_url.split("://")[1].split(":")[0]
                except Exception as e:
                    print(f"ERROR: Failed to extract CM host from URL {self.cm_client.base_url}: {e}", file=sys.stderr)
                    return False
                
                # Validate keytab file exists on CM host
                keytab_check_cmd = f"test -f {self.keytab} && echo 'EXISTS' || echo 'NOT_FOUND'"
                result = self._run_remote_command(cm_host, keytab_check_cmd)
                
                if result.returncode != 0 or 'NOT_FOUND' in result.stdout:
                    print(f"ERROR: Keytab file not found on CM host {cm_host}: {self.keytab}", file=sys.stderr)
                    return False
                
                print(f"[>] Using keytab: {self.keytab}")
                print(f"[>] Using principal: {self.principal}")
                
                # Test kinit on CM host
                if not self.dry_run:
                    kinit_cmd = f"kinit -kt {self.keytab} {self.principal}"
                    result = self._run_remote_command(cm_host, kinit_cmd)
                    
                    if result.returncode != 0:
                        print(f"ERROR: Failed to authenticate with Kerberos: {result.stderr}", file=sys.stderr)
                        return False
                    
                    print("[>] Kerberos authentication successful")
            else:
                print("[>] Ozone security is disabled - no Kerberos authentication required")
            
            return True
            
        except Exception as e:
            print(f"ERROR: Failed to check security configuration: {e}", file=sys.stderr)
            return False
    
    def run_bootstrap(self) -> bool:
        """Run the complete bootstrap process"""
        try:
            print("=" * 80)
            print("OZONE MANAGER BOOTSTRAP AUTOMATION")
            print("=" * 80)
            
            # Step 1.0: Find healthy leader
            if not self.discover_cluster_info():
                return False
            
            # Step 1.1: Check security configuration (do this before any ozone commands)
            if not self.check_security_configuration():
                return False
            
            # Step 1.2: Prompt user to confirm no snapshots exist
            if not self.check_snapshots_and_prompt():
                return False
            
            # Step 1.3: Get OM roles from CLI
            if not self.get_om_roles_from_cli():
                return False
            
            # Step 1.4: Get configuration (after we have the follower role identified)
            if not self.get_om_configuration():
                return False
            
            # Step 2.0: Check OM leader health
            if not self.verify_leader_health():
                return False
            
            # Step 2.1: List last Ratis log files before bootstrapping
            print("[2.1] Listing Ratis log files before bootstrapping...")
            
            # Debug: Check if leader and follower are the same
            if self.leader_host == self.follower_host:
                print(f"[DEBUG] Leader and follower are the same host: {self.leader_host}")
                # Only list once if they're the same
                if not self.list_last_ratis_log("before", self.leader_host):
                    print("WARNING: Failed to list Ratis log file before bootstrapping", file=sys.stderr)
            else:
                # List leader Ratis log
                if not self.list_last_ratis_log("before", self.leader_host):
                    print("WARNING: Failed to list leader Ratis log file before bootstrapping", file=sys.stderr)
                
                # List follower Ratis log
                if not self.list_last_ratis_log("before", self.follower_host):
                    print("WARNING: Failed to list follower Ratis log file before bootstrapping", file=sys.stderr)
            
            # Step 3.0: Stop follower
            if not self.stop_follower():
                return False
            
            # Step 4.0: Test checkpoint endpoint
            if not self.test_checkpoint_endpoint():
                return False
            
            # Step 4.1: Download checkpoint
            if not self.download_checkpoint():
                return False
            
            # Step 4.2: Extract checkpoint
            if not self.extract_checkpoint():
                return False
            
            # Step 5.0: Backup and replace database
            if not self.backup_and_replace_database():
                return False
            
            # Step 5.1: Backup Ratis logs
            if not self.backup_ratis_logs():
                return False
            
            # Step 6.0: Start follower
            if not self.start_follower():
                return False
            
            # Step 7.0: Verify status
            if not self.verify_om_status():
                return False
            
            # Step 7.1: List last Ratis log files after bootstrapping
            print("[7.1] Listing Ratis log files after bootstrapping...")
            
            # Debug: Check if leader and follower are the same
            if self.leader_host == self.follower_host:
                print(f"[DEBUG] Leader and follower are the same host: {self.leader_host}")
                # Only list once if they're the same
                if not self.list_last_ratis_log("after", self.leader_host):
                    print("WARNING: Failed to list Ratis log file after bootstrapping", file=sys.stderr)
            else:
                # List leader Ratis log
                if not self.list_last_ratis_log("after", self.leader_host):
                    print("WARNING: Failed to list leader Ratis log file after bootstrapping", file=sys.stderr)
                
                # List follower Ratis log
                if not self.list_last_ratis_log("after", self.follower_host):
                    print("WARNING: Failed to list follower Ratis log file after bootstrapping", file=sys.stderr)
            
            # Step 8.0: Restart OM role if it was stopped during bootstrap
            if self.om_role_was_stopped:
                print("[8.0] Restarting OM role that was stopped during bootstrap...")
                if not self.start_follower():
                    print("WARNING: Failed to restart OM role", file=sys.stderr)
                else:
                    print("[>] OM role restarted successfully")
            
            print("=" * 80)
            print("BOOTSTRAP PROCESS COMPLETED SUCCESSFULLY")
            print("=" * 80)
            return True
            
        except Exception as e:
            print(f"ERROR: Bootstrap process failed: {e}", file=sys.stderr)
            return False
        finally:
            self.cleanup()


def main():
    parser = argparse.ArgumentParser(
        description="Ozone Manager Bootstrap Automation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List available clusters
  python ozone_om_bootstrap.py --cm-base-url https://cm:7183 --list-clusters --insecure

  # Run bootstrap with dry-run to see what would happen (unsecured cluster)
  python ozone_om_bootstrap.py --cm-base-url https://cm:7183 --cluster "Cluster 1" \\
    --follower-host om-node-2.example.com --insecure --dry-run

  # Run bootstrap with dry-run (secured cluster with Kerberos)
  python ozone_om_bootstrap.py --cm-base-url https://cm:7183 --cluster "Cluster 1" \\
    --follower-host om-node-2.example.com --keytab /etc/security/keytabs/om.keytab --principal om/om-node-2.example.com@REALM \\
    --insecure --dry-run

  # Run actual bootstrap (unsecured cluster)
  python ozone_om_bootstrap.py --cm-base-url https://cm:7183 --cluster "Cluster 1" \\
    --follower-host om-node-2.example.com --insecure --yes

  # Run actual bootstrap (secured cluster with Kerberos)
  python ozone_om_bootstrap.py --cm-base-url https://cm:7183 --cluster "Cluster 1" \\
    --follower-host om-node-2.example.com --keytab /etc/security/keytabs/om.keytab --principal om/om-node-2.example.com@REALM \\
    --insecure --yes
        """
    )
    
    parser.add_argument("--cm-base-url", required=True,
                       help="Cloudera Manager base URL (e.g., https://cm:7183)")
    parser.add_argument("--cluster",
                       help="Cluster name (required unless --list-clusters is used)")
    parser.add_argument("--follower-host",
                       help="Hostname of the follower OM to bootstrap")
    parser.add_argument("--username", default="admin",
                       help="CM username (default: admin)")
    parser.add_argument("--password", default="admin",
                       help="CM password (default: admin)")
    parser.add_argument("--insecure", action="store_true",
                       help="Skip SSL verification")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be done without executing")
    parser.add_argument("--yes", action="store_true",
                       help="Confirm execution (required for non-dry-run)")
    parser.add_argument("--list-clusters", action="store_true",
                       help="List available clusters and exit")
    parser.add_argument("--keytab",
                       help="Path to Kerberos keytab file on remote host (required when Ozone security is enabled)")
    parser.add_argument("--principal",
                       help="Kerberos principal (required when Ozone security is enabled)")
    
    args = parser.parse_args()
    
    # Create CM client
    cm_client = CmClient(
        base_url=args.cm_base_url,
        api_version="v49",
        username=args.username,
        password=args.password,
        verify=not args.insecure
    )
    
    # Handle list clusters option
    if args.list_clusters:
        clusters = cm_client.discover_clusters()
        if not clusters:
            sys.exit(1)
        sys.exit(0)
    
    # Validate required arguments for bootstrap
    if not args.cluster:
        print("ERROR: --cluster is required for bootstrap operations", file=sys.stderr)
        print("Use --list-clusters to see available clusters", file=sys.stderr)
        sys.exit(1)
    
    if not args.follower_host:
        print("ERROR: --follower-host is required for bootstrap operations", file=sys.stderr)
        sys.exit(1)
    
    # Validate arguments
    if not args.dry_run and not args.yes:
        print("ERROR: --yes is required for non-dry-run execution", file=sys.stderr)
        sys.exit(1)
    
    # Create bootstrap instance
    bootstrap = OzoneOMBootstrap(
        cm_client=cm_client,
        cluster_name=args.cluster,
        follower_host=args.follower_host,
        insecure=args.insecure,
        dry_run=args.dry_run,
        keytab=args.keytab,
        principal=args.principal
    )
    
    # Run bootstrap
    success = bootstrap.run_bootstrap()
    
    if success:
        print("Bootstrap completed successfully!")
        sys.exit(0)
    else:
        print("Bootstrap failed!", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
