#!/usr/bin/env python3

#
# Copyright (C) 2025 Cloudera, Inc. All Rights Reserved.
#
# See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.
#
# Cloudera, Inc. licenses this file to you under the
# Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
    ./ozone_om_bootstrap.py --cm-base-url https://<cm>:7183 --cluster "Cluster 1" \
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
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, quote

# Suppress SSL warnings
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", "..", ".."))
(sys.path.insert(0, _REPO_ROOT) if _REPO_ROOT not in sys.path else None)

from common.cm_client import CmClient


# Module logger and logging setup
logger = logging.getLogger("ozone.om.bootstrap")

_LOG_TS: Optional[str] = None
_CONSOLE_HANDLER: Optional[logging.Handler] = None
_FILE_HANDLER: Optional[logging.Handler] = None

def _configure_logging() -> None:
    """Configure logging with one console handler and one file handler at
    logs/ozone_om_bootstrap_YYYYMMDD_HHMMSS.log.
    """
    global _LOG_TS, _CONSOLE_HANDLER, _FILE_HANDLER
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return

    logs_dir = Path(_SCRIPT_DIR) / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    if _LOG_TS is None:
        _LOG_TS = time.strftime("%Y%m%d_%H%M%S")

    main_log_file = logs_dir / f"ozone_om_bootstrap_{_LOG_TS}.log"

    root_logger.setLevel(logging.INFO)

    console_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    # Console (stream) handler
    _CONSOLE_HANDLER = logging.StreamHandler()
    _CONSOLE_HANDLER.setFormatter(console_formatter)

    # Main file handler
    _FILE_HANDLER = logging.FileHandler(str(main_log_file))
    _FILE_HANDLER.setFormatter(file_formatter)

    root_logger.addHandler(_CONSOLE_HANDLER)
    root_logger.addHandler(_FILE_HANDLER)


class OzoneOMBootstrap:
    """Ozone Manager Bootstrap automation class"""
    
    def __init__(self, cm_client: CmClient, cluster_name: str, follower_host: str, 
                 insecure: bool = False, dry_run: bool = False, keytab: str = None, principal: str = None,
                 ssh_user: str = None, sudo_user: str = None,
                 enable_leadership_transfer_test: bool = False):
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
        
        # SSH and sudo configuration
        self.ssh_user = ssh_user
        self.sudo_user = sudo_user
        
        # Behavior flags
        self.enable_leadership_transfer_test = enable_leadership_transfer_test
    
    def validate_ssh_connectivity(self) -> bool:
        """Validate SSH connectivity to CM host and all OM role nodes"""
        logger.info("[1.1] [SSH VALIDATION] Validating SSH connectivity...")
        
        # Get CM host from base URL
        try:
            cm_host = self.cm_client.base_url.split("://")[1].split(":")[0]
            logger.info(f"[>] CM host: {cm_host}")
        except Exception as e:
            logger.error(f"Failed to extract CM host from URL {self.cm_client.base_url}: {e}")
            return False
        
        # Collect all hosts to validate
        hosts_to_validate = [cm_host]
        
        # Add OM role hosts if we have them
        if self.om_roles:
            for role in self.om_roles:
                host_id = role.get("hostRef", {}).get("hostId")
                if host_id:
                    host_info = self.cm_client.get_host_by_id(host_id)
                    hostname = host_info.get("hostname")
                    if hostname and hostname not in hosts_to_validate:
                        hosts_to_validate.append(hostname)
        
        logger.info(f"[>] Validating SSH connectivity to {len(hosts_to_validate)} hosts...")
        
        # Validate SSH connectivity to each host
        for host in hosts_to_validate:
            if not self._test_ssh_connection(host):
                logger.error(f"SSH connection failed to {host}")
                return False
            logger.info(f"[>] SSH connection to {host}: OK")
        
        logger.info("[>] All SSH connections validated successfully")
        return True
    
    def _test_ssh_connection(self, host: str) -> bool:
        """Test SSH connection to a specific host"""
        ssh_cmd = ["ssh", "-o", "ConnectTimeout=10", "-o", "BatchMode=yes"]
        
        if self.ssh_user:
            ssh_cmd.extend(["-l", self.ssh_user])
        
        ssh_cmd.extend([host, "echo", "SSH connection successful"])
        
        try:
            result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=15)
            return result.returncode == 0
        except subprocess.TimeoutExpired:
            logger.warning(f"  SSH connection to {host} timed out")
            return False
        except Exception as e:
            logger.error(f"  SSH connection to {host} failed: {e}")
            return False
    
    def validate_sudo_access(self) -> bool:
        """Validate sudo access and determine sudo user if needed"""
        logger.info("[SUDO VALIDATION] Validating sudo access...")
        
        # Skip sudo validation if SSH user is root
        if self.ssh_user == "root":
            logger.info("[>] SSH user is root, skipping sudo validation")
            return True
        
        # Get CM host for sudo validation
        try:
            cm_host = self.cm_client.base_url.split("://")[1].split(":")[0]
        except Exception as e:
            logger.error(f"Failed to extract CM host from URL {self.cm_client.base_url}: {e}")
            return False
        
        # Test sudo access on CM host
        if not self._test_sudo_access(cm_host):
            logger.warning(f"Sudo access failed on {cm_host}")
            
            # Ask user for sudo user if not provided
            if not self.sudo_user:
                logger.info("\n" + "="*60)
                logger.info("SUDO ACCESS REQUIRED")
                logger.info("="*60)
                logger.info("Sudo access is required to run commands on the remote hosts.")
                logger.info("Please provide a sudo-enabled user account.")
                logger.info("="*60)
                
                try:
                    sudo_user_input = input("Enter sudo user (or press Enter to abort): ").strip()
                    if sudo_user_input:
                        self.sudo_user = sudo_user_input
                        logger.info(f"[>] Using sudo user: {self.sudo_user}")
                    else:
                        logger.info("[>] Aborting due to no sudo user provided")
                        return False
                except KeyboardInterrupt:
                    logger.info("\n[>] Operation aborted by user")
                    return False
            
            # Test sudo access with the provided user
            if not self._test_sudo_access_with_user(cm_host, self.sudo_user):
                logger.error(f"Sudo access failed with user {self.sudo_user} on {cm_host}")
                return False
        
        logger.info(f"[>] Sudo access validated successfully")
        return True
    
    def _test_sudo_access(self, host: str) -> bool:
        """Test sudo access on a specific host"""
        ssh_cmd = ["ssh", "-o", "ConnectTimeout=10", "-o", "BatchMode=yes"]
        
        if self.ssh_user:
            ssh_cmd.extend(["-l", self.ssh_user])
        
        ssh_cmd.extend([host, "sudo", "-n", "echo", "sudo access successful"])
        
        try:
            result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=15)
            return result.returncode == 0
        except subprocess.TimeoutExpired:
            return False
        except Exception as e:
            return False
    
    def _test_sudo_access_with_user(self, host: str, sudo_user: str) -> bool:
        """Test sudo access with a specific user on a specific host"""
        ssh_cmd = ["ssh", "-o", "ConnectTimeout=10", "-o", "BatchMode=yes"]
        
        if self.ssh_user:
            ssh_cmd.extend(["-l", self.ssh_user])
        
        ssh_cmd.extend([host, f"sudo -u {sudo_user} -n echo 'sudo access successful'"])
        
        try:
            result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=15)
            return result.returncode == 0
        except subprocess.TimeoutExpired:
            return False
        except Exception as e:
            return False
        
    def discover_cluster_info(self) -> bool:
        """Discover Ozone service and OM roles information"""
        logger.info("[1.0] Discovering Ozone service information...")
        
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
                logger.error(f"Cluster '{self.cluster_name}' not found in Cloudera Manager")
                logger.info("Available clusters:")
                for cluster_name in available_clusters:
                    logger.info(f"  - '{cluster_name}'")
                logger.info("\nUse the exact cluster name from the list above.")
                return False
                
        except Exception as e:
            logger.error(f"Failed to connect to Cloudera Manager: {e}")
            return False
        
        # Find Ozone service
        services = self.cm_client.list_services(self.cluster_name)
        ozone_services = [s for s in services if s.get("type") == "OZONE"]
        
        if not ozone_services:
            logger.error("No Ozone service found in cluster")
            logger.info("Available services:")
            for service in services:
                service_name = service.get('name', 'Unknown')
                service_type = service.get('type', 'Unknown')
                logger.info(f"  - {service_name} ({service_type})")
            return False
            
        self.ozone_service = ozone_services[0]["name"]
        logger.info(f"[>] Ozone service: {self.ozone_service}")
        
        # Get OM roles
        roles = self.cm_client.list_roles(self.cluster_name, self.ozone_service)
        om_roles = [r for r in roles if r.get("type") == "OZONE_MANAGER"]
        
        if not om_roles:
            logger.error("No Ozone Manager roles found")
            logger.info("Available roles:")
            for role in roles:
                role_name = role.get('name', 'Unknown')
                role_type = role.get('type', 'Unknown')
                logger.info(f"  - {role_name} ({role_type})")
            return False
            
        self.om_roles = om_roles
        logger.info(f"[>] Found {len(om_roles)} OM roles")
        
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
                logger.warning("Could not determine service ID from CM config")
                logger.info("Will try to discover service ID from ozone command output")
        except Exception as e:
            logger.warning(f"Could not get service config: {e}")
            
        return True
    
    def get_om_roles_from_cli(self) -> bool:
        """Get OM roles using ozone admin command"""
        logger.info("[1.3] Getting OM roles from ozone admin command...")
        
        # Use the CM host from cm-base-url
        try:
            command_host = self.cm_client.base_url.split("://")[1].split(":")[0]
            logger.info(f"[>] Using CM host for ozone commands: {command_host}")
        except Exception as e:
            logger.error(f"Failed to extract CM host from URL {self.cm_client.base_url}: {e}")
            return False
        
        # Run ozone admin om roles command
        if self.service_id:
            cmd = f"ozone admin om roles -id={self.service_id}"
        else:
            # Try to get service ID first
            logger.warning("No service ID available, trying to discover it...")
            service_id_cmd = "ozone getconf -confKey ozone.service.id"
            service_id_result = self._run_remote_command(command_host, service_id_cmd)
            
            if service_id_result.returncode == 0 and service_id_result.stdout.strip():
                self.service_id = service_id_result.stdout.strip()
                logger.info(f"[>] Discovered service ID: {self.service_id}")
                cmd = f"ozone admin om roles -id={self.service_id}"
            else:
                logger.error("Could not discover service ID, cannot proceed")
                return False
        
        result = self._run_remote_command(command_host, cmd)
        
        if result.returncode != 0:
            logger.error(f"Failed to get OM roles: {result.stderr}")
            return False
        
        # Parse roles output
        roles_output = result.stdout
        logger.info(f"[>] OM roles output:\n{roles_output}")
        
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
                logger.warning("Could not parse leader from roles output")
        else:
            self.leader_host = leader_match.group(1)
        
        if self.leader_host:
            logger.info(f"[>] Leader OM: {self.leader_host}")
        
        # Parse followers - handle both formats
        follower_matches = re.findall(r'FOLLOWER:\s*([^\s]+)', roles_output)
        if not follower_matches:
            # Try alternative format: nodeId : FOLLOWER (hostname)
            follower_matches = re.findall(r'(\w+)\s*:\s*FOLLOWER\s*\(([^)]+)\)', roles_output)
            if follower_matches:
                # Extract just the hostnames from the matches
                follower_matches = [match[1] for match in follower_matches]
        
        if follower_matches:
            logger.info(f"[>] Follower OMs: {follower_matches}")
            if self.follower_host in follower_matches:
                logger.info(f"[>] Target follower: {self.follower_host}")
                
                # Find the follower role from CM roles
                for role in self.om_roles:
                    host_id = role.get("hostRef", {}).get("hostId")
                    if host_id:
                        host_info = self.cm_client.get_host_by_id(host_id)
                        if host_info.get("hostname") == self.follower_host:
                            self.follower_role = role
                            logger.info(f"[>] Identified follower role: {role.get('name')}")
                            break
                
                if not self.follower_role:
                    logger.warning(f"Could not identify follower role for {self.follower_host}")
            else:
                logger.warning(f"Target follower {self.follower_host} not found in follower list")
                return False
        
        return True
    
    def verify_leader_health(self) -> bool:
        """Verify the leader OM is healthy and perform leader switch if needed"""
        logger.info("[2.0] Verifying leader OM health...")
        
        if not self.leader_host:
            logger.error("No leader host identified")
            return False
        
        # Check if leader is healthy by running a simple command
        if not self.service_id:
            logger.error("Service ID required for health check")
            return False
        health_cmd = f"ozone admin om getserviceroles --service-id={self.service_id}"
        result = self._run_remote_command(self.leader_host, health_cmd)
        
        if result.returncode != 0:
            logger.warning(f"Leader {self.leader_host} appears unhealthy, attempting leader switch...")
            
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
                            logger.info(f"[>] Switching leadership to healthy follower: {candidate_host}")
                            
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
                                    logger.warning(f"Could not extract node ID for candidate {candidate_host}")
                                    continue
                                transfer_cmd = f"ozone admin om transfer -id={self.service_id} -n {node_id}"
                                transfer_result = self._run_remote_command(candidate_host, transfer_cmd)
                                
                                if transfer_result.returncode == 0:
                                    self.leader_host = candidate_host
                                    logger.info(f"[>] Successfully switched leadership to {candidate_host}")
                                    time.sleep(30)  # Wait for leadership transfer to complete
                                    break
                                else:
                                    logger.warning(f"Failed to transfer leadership: {transfer_result.stderr}")
        
        logger.info(f"[>] Using leader: {self.leader_host}")
        return True
    
    def stop_follower(self) -> bool:
        """Stop the target follower OM"""
        
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
            logger.error(f"Could not find role name for follower {self.follower_host}")
            return False
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would stop role: {follower_role_name}")
            return True
        
        try:
            result = self.cm_client.role_command(
                self.cluster_name, 
                self.ozone_service, 
                "stop", 
                [follower_role_name]
            )
            logger.info(f"[>] Stop command initiated: {result}")
            
            # Wait for the process to stop by checking for the OzoneManagerStarter process
            logger.info(f"[>] Waiting for OzoneManagerStarter process to stop...")
            max_wait_time = 120  # 2 minutes max wait
            wait_interval = 5    # Check every 5 seconds
            elapsed_time = 0
            
            while elapsed_time < max_wait_time:
                # Check if the OzoneManagerStarter process is still running
                check_cmd = f"pgrep -f 'org.apache.hadoop.ozone.om.OzoneManagerStarter'"
                check_result = self._run_remote_command(self.follower_host, check_cmd)
                
                if check_result.returncode != 0:
                    # Process not found, it has stopped
                    logger.info(f"[>] OzoneManagerStarter process stopped successfully (after {elapsed_time} seconds)")
                    self.om_role_was_stopped = True
                    return True
                
                # Process still running, wait and check again
                time.sleep(wait_interval)
                elapsed_time += wait_interval
                logger.info(f"[>] Process still running, waiting... ({elapsed_time}/{max_wait_time} seconds)")
            
            # If we reach here, the process didn't stop within the timeout
            logger.warning(f"OzoneManagerStarter process did not stop within {max_wait_time} seconds")
            # print(f"[>] Proceeding anyway, assuming process will stop soon...")
            self.om_role_was_stopped = False
            return False
            
        except Exception as e:
            logger.error(f"Failed to stop follower OM: {e}")
            return False
    
    def get_om_configuration(self) -> bool:
        """Get OM configuration including database and ratis directories"""
        logger.info("[1.4] Getting OM configuration...")

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
                logger.error("Follower role not identified yet")
                return False
            
            follower_role_name = self.follower_role.get("name")
            if not follower_role_name:
                logger.error("Follower role name not found")
                return False
            
            # Get configuration for the specific follower role
            role_cfg = self.cm_client.get_role_config(self.cluster_name, self.ozone_service, follower_role_name, view="FULL")
            role_items = role_cfg.get("items", [])
            # print(f"[DEBUG] Follower role config has {len(role_items)} items")
            
            # If role-specific config is empty, fall back to role config group
            if not role_items:
                logger.info(f"[>] No role-specific config found for {follower_role_name}, using role config group...")
                groups = self.cm_client.list_role_config_groups(self.cluster_name, self.ozone_service)
                om_groups = [g for g in groups if (g.get("roleType") or g.get("roleTypeName") or "").upper() == "OZONE_MANAGER"]
                if not om_groups:
                    logger.error("No OZONE_MANAGER role config groups found")
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
                logger.info(f"[>] Using role-specific configuration for {follower_role_name}")

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
            logger.info(f"[>] OM DB directory: {self.om_db_dir}")
            logger.info(f"[>] Ratis directory: {self.ratis_dir}")
            logger.info(f"[>] HTTP Kerberos enabled: {self.ozone_http_kerberos_enabled}")
            logger.info(f"[>] Using protocol: {self.om_protocol}")
            logger.info(f"[>] Using port: {self.om_port}")

            # Minimal required: OM DB dir and Ratis dir
            if not self.om_db_dir:
                logger.error("Could not determine 'ozone.om.db.dirs' from global configs")
                return False
            if not self.ratis_dir:
                logger.warning("Could not determine 'ozone.om.ratis.storage.dir' from global configs")

            return True

        except Exception as e:
            logger.error(f"Failed to get OM configuration: {e}")
            return False
    
    def download_checkpoint(self) -> bool:
        """Download checkpoint from leader OM"""
        logger.info("[4.1] Downloading checkpoint from leader OM...")
        
        if not self.leader_host:
            logger.error("No leader host available")
            return False
        
        # Use the protocol and port determined from configuration
        protocol = getattr(self, 'om_protocol', 'http')
        port = getattr(self, 'om_port', '9874')
        
        # Create temporary directory on remote host for download with epoch timestamp
        temp_dir_cmd = f"mktemp -d /tmp/om_bootstrap_{self.epoch_time}_XXXXXX"
        temp_dir_result = self._run_remote_command(self.follower_host, temp_dir_cmd)
        
        if temp_dir_result.returncode != 0:
            logger.error(f"Failed to create temporary directory on remote host: {temp_dir_result.stderr}")
            return False
        
        self.temp_dir = temp_dir_result.stdout.strip()
        checkpoint_file = f"{self.temp_dir}/om-db-checkpoint.tar"
        
        # print(f"[DEBUG] Created temporary directory on remote host: {self.temp_dir}")
        
        # Build curl command
        curl_cmd = ["curl", "-k", "-s"]  # Use silent mode instead of verbose
        
        # Add Kerberos authentication if enabled
        if self.ozone_http_kerberos_enabled:
            if not self.keytab or not self.principal:
                logger.error("HTTP Kerberos is enabled but --keytab and --principal are required")
                return False
            
            # Add negotiate authentication
            curl_cmd.extend(["--negotiate", "-u", ":"])
            logger.info(f"[>] Using Kerberos authentication for HTTP request")
        
        curl_cmd.extend([
            f"{protocol}://{self.leader_host}:{port}/dbCheckpoint?flushBeforeCheckpoint=true",
            "-o", checkpoint_file
        ])
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would run: {' '.join(curl_cmd)}")
            return True
        
        # Run curl command on follower host
        result = self._run_remote_command(self.follower_host, ' '.join(curl_cmd))
        
        if result.returncode != 0:
            logger.error(f"Failed to download checkpoint: {result.stderr}")
            return False
        
        # Verify file was downloaded and check its content
        ls_cmd = f"ls -la {checkpoint_file}"
        ls_result = self._run_remote_command(self.follower_host, ls_cmd)
        
        if ls_result.returncode != 0:
            logger.error(f"Checkpoint file not found after download")
            return False
        
        logger.info(f"[>] Checkpoint downloaded: {ls_result.stdout.strip()}")
        
        # Check file type and content
        file_cmd = f"file {checkpoint_file}"
        file_result = self._run_remote_command(self.follower_host, file_cmd)
        
        if file_result.returncode == 0:
            logger.info(f"[>] File type: {file_result.stdout.strip()}")
        
        # Check first few bytes to see if it's a tar file
        head_cmd = f"head -c 32 {checkpoint_file} | hexdump -C"
        head_result = self._run_remote_command(self.follower_host, head_cmd)
        
        if head_result.returncode == 0:
            logger.info(f"[>] File header (first 32 bytes):\n{head_result.stdout}")
        
        # Check if the file contains error messages (common with HTTP errors)
        grep_cmd = f"grep -i 'error\\|exception\\|failed' {checkpoint_file} | head -5"
        grep_result = self._run_remote_command(self.follower_host, grep_cmd)
        
        if grep_result.returncode == 0 and grep_result.stdout.strip():
            logger.warning(f"File contains error messages: {grep_result.stdout.strip()}")
        
        # Test if it's a valid tar file
        tar_test_cmd = f"tar -tf {checkpoint_file} > /dev/null 2>&1"
        tar_test_result = self._run_remote_command(self.follower_host, tar_test_cmd)
        
        if tar_test_result.returncode != 0:
            logger.error(f"Downloaded file is not a valid tar archive")
            logger.error(f"This suggests the checkpoint download failed or returned an error response")
            
            return False
        
        logger.info(f"[>] Checkpoint file is a valid tar archive")
        return True
    
    def list_last_ratis_log(self, stage: str = "current", host: str = None) -> bool:
        """List the last Ratis log file from the Ratis log directory"""
        if not host:
            host = self.follower_host
            
        host_label = "LEADER" if host == self.leader_host else "FOLLOWER"
        logger.info(f"[{stage.upper()}] Listing last Ratis log file on {host_label} ({host})...")
        
        if not self.ratis_dir:
            logger.error("Ratis directory not configured")
            return False
        
        # Find the last log file in the Ratis directory
        # Handle the structure: /var/lib/hadoop-ozone/om/ratis/{uuid}/current/
        # Look for log_* files (including log_inprogress_*)
        find_cmd = f"find {self.ratis_dir} -name 'log_*' -type f -printf '%T@ %p\n' | sort -n | tail -1"
        find_result = self._run_remote_command(host, find_cmd)
        
        if find_result.returncode != 0 or not find_result.stdout.strip():
            logger.warning(f"No log_* files found in Ratis directory on {host}: {self.ratis_dir}")
            return False
        
        # Extract the file path (remove timestamp)
        last_log_file = find_result.stdout.strip().split(' ', 1)[1]
        
        # Get file details
        ls_cmd = f"ls -la {last_log_file}"
        ls_result = self._run_remote_command(host, ls_cmd)
        
        if ls_result.returncode == 0:
            logger.info(f"[>] {host_label} Ratis log file: {ls_result.stdout.strip()}")
            
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
                    logger.info(f"[>] {host_label} Ratis log file changed during bootstrap:")
                    logger.info(f"    Before: {before_log}")
                    logger.info(f"    After:  {last_log_file}")
                elif before_log:
                    logger.info(f"[>] {host_label} Ratis log file unchanged during bootstrap")
            
            return True
        else:
                    logger.error(f"Failed to get file details on {host}: {ls_result.stderr}")
        return False
    
    def check_snapshots_and_prompt(self) -> bool:
        """Prompt user to confirm there are no snapshots in the system"""
        # Print snapshot confirmation to console without logger metadata
        print("\n" + "="*80)
        print("** [1.2] SNAPSHOT CONFIRMATION REQUIRED **")
        print("="*80)
        print("** STOP! Do not proceed if the cluster enables Ozone Snapshot.\n"
              "Contact Cloudera Storage Engineering team for further instructions if that is the case. **")
        print("="*80)
        print("Before proceeding, please confirm that:")
        print("1. You have NO snapshots in the Ozone system")
        print("2. You understand that bootstrap operations may impact snapshots")
        print("3. You have backed up any important data")
        print("="*80)
        print("Type 'Continue' (exactly as shown) to proceed with the bootstrap operation.")
        print("Any other input will abort the operation.")
        print("="*80)
        
        try:
            user_input = input("Enter 'Continue' to proceed: ").strip()
            if user_input == "Continue":
                logger.info("[>] User confirmed to proceed with bootstrap operation")
                return True
            else:
                logger.info("[>] User aborted the operation")
                return False
        except KeyboardInterrupt:
            logger.info("\n[>] Operation aborted by user")
            return False
    
    def test_checkpoint_endpoint(self) -> bool:
        """Test the checkpoint endpoint to ensure it's working"""
        logger.info("[4.0] Testing checkpoint endpoint...")
        
        if not self.leader_host or not self.om_protocol or not self.om_port:
            logger.error("Missing leader host or OM configuration")
            return False
        
        # Test the endpoint with a HEAD request first
        test_cmd = ["curl", "-k", "-I", "-s"]  # Added -s for silent mode
        
        # Add Kerberos authentication if enabled
        if self.ozone_http_kerberos_enabled:
            if not self.keytab or not self.principal:
                logger.error("HTTP Kerberos is enabled but --keytab and --principal are required")
                return False
            
            # Add negotiate authentication
            test_cmd.extend(["--negotiate", "-u", ":"])
            logger.info(f"[>] Using Kerberos authentication for endpoint test")
        
        test_cmd.append(f"{self.om_protocol}://{self.leader_host}:{self.om_port}/dbCheckpoint")
        test_cmd_str = ' '.join(test_cmd)
        test_result = self._run_remote_command(self.follower_host, test_cmd_str)
        
        if test_result.returncode != 0:
            logger.error(f"Checkpoint endpoint test failed: {test_result.stderr}")
            return False
        
        # Extract just the HTTP status line
        lines = test_result.stdout.strip().split('\n')
        status_line = None
        for line in lines:
            if line.startswith('HTTP/'):
                status_line = line.strip()
                break
        
        if status_line:
            logger.info(f"[>] Checkpoint endpoint test successful: {status_line}")
        else:
            logger.info(f"[>] Checkpoint endpoint test successful")
        
        return True
    
    def extract_checkpoint(self) -> bool:
        """Extract checkpoint to temporary directory"""
        logger.info("[4.2] Extracting checkpoint...")
        
        if not self.om_db_dir or not self.temp_dir:
            logger.error("Missing OM DB directory or temp directory")
            return False
        
        checkpoint_file = f"{self.temp_dir}/om-db-checkpoint.tar"
        extract_dir = f"{self.om_db_dir}.tmp_{self.epoch_time}"
        
        # Create temporary directory
        mkdir_cmd = f"mkdir -p {extract_dir}"
        mkdir_result = self._run_remote_command(self.follower_host, mkdir_cmd)
        
        if mkdir_result.returncode != 0:
            logger.error(f"Failed to create extract directory: {mkdir_result.stderr}")
            return False
        
        # Extract checkpoint
        tar_cmd = f"tar -xvf {checkpoint_file} -C {extract_dir}"
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would run: {tar_cmd}")
            return True
        
        tar_result = self._run_remote_command(self.follower_host, tar_cmd)
        
        if tar_result.returncode != 0:
            logger.error(f"Failed to extract checkpoint: {tar_result.stderr}")
            logger.error(f"This usually means the downloaded file is not a valid tar archive")
            logger.error(f"Please check the download step output above for more details")
            return False
        
        logger.info(f"[>] Checkpoint extracted to {extract_dir}")
        return True
    
    def backup_and_replace_database(self) -> bool:
        """Backup current database and replace with checkpoint"""
        logger.info("[5.0] Backing up and replacing OM database...")
        
        if not self.om_db_dir:
            logger.error("OM DB directory not configured")
            return False
        
        # Create backup directory
        backup_cmd = f"mkdir -p {self.backup_dir}"
        backup_result = self._run_remote_command(self.follower_host, backup_cmd)
        
        if backup_result.returncode != 0:
            logger.error(f"Failed to create backup directory: {backup_result.stderr}")
            return False
        
        # Backup current database
        current_db = f"{self.om_db_dir}/om.db"
        backup_db = f"{self.backup_dir}/om.db.backup.{self.epoch_time}"
        backup_current_cmd = f"cp -r {current_db} {backup_db}"
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would run: {backup_current_cmd}")
        else:
            backup_current_result = self._run_remote_command(self.follower_host, backup_current_cmd)
            if backup_current_result.returncode != 0:
                logger.error(f"Failed to backup current database: {backup_current_result.stderr}")
                return False
        
        # Move current database to backup with epoch timestamp
        move_current_cmd = f"mv {current_db} {current_db}.backup.{self.epoch_time}"
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would run: {move_current_cmd}")
        else:
            move_current_result = self._run_remote_command(self.follower_host, move_current_cmd)
            if move_current_result.returncode != 0:
                logger.error(f"Failed to move current database: {move_current_result.stderr}")
                return False
        
        # Move new checkpoint into place
        new_db = f"{self.om_db_dir}.tmp_{self.epoch_time}"
        move_new_cmd = f"mv {new_db} {current_db}"
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would run: {move_new_cmd}")
        else:
            move_new_result = self._run_remote_command(self.follower_host, move_new_cmd)
            if move_new_result.returncode != 0:
                logger.error(f"Failed to move new database: {move_new_result.stderr}")
                return False
        
        # Set correct ownership
        chown_cmd = f"chown -R hdfs:hdfs {current_db}"
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would run: {chown_cmd}")
        else:
            chown_result = self._run_remote_command(self.follower_host, chown_cmd)
            if chown_result.returncode != 0:
                logger.warning(f"Failed to set ownership: {chown_result.stderr}")
        
        logger.info(f"[>] Database replaced successfully")
        return True
    
    def backup_ratis_logs(self) -> bool:
        """Backup Ratis logs"""
        logger.info("[5.1] Backing up Ratis logs...")
        
        if not self.ratis_dir:
            logger.warning("Ratis directory not configured, skipping Ratis log backup")
            return True
        
        # Find the Raft group directory
        find_group_cmd = f"find {self.ratis_dir} -name 'current' -type d | head -1"
        find_result = self._run_remote_command(self.follower_host, find_group_cmd)
        
        if find_result.returncode != 0 or not find_result.stdout.strip():
            logger.warning(f"Could not find Ratis current directory")
            return True
        
        current_dir = find_result.stdout.strip()
        group_dir = os.path.dirname(current_dir)
        
        # Create backup directories with epoch timestamp
        backup_ratis_cmd = f"mkdir -p {self.backup_dir}/ratisLogs_{self.epoch_time} {self.backup_dir}/ratisLogs_{self.epoch_time}/original"
        backup_ratis_result = self._run_remote_command(self.follower_host, backup_ratis_cmd)
        
        if backup_ratis_result.returncode != 0:
            logger.error(f"Failed to create Ratis backup directories: {backup_ratis_result.stderr}")
            return False
        
        # Backup current Raft logs
        backup_logs_cmd = f"cp {current_dir}/log* {self.backup_dir}/ratisLogs_{self.epoch_time}/ 2>/dev/null || true"
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would run: {backup_logs_cmd}")
        else:
            backup_logs_result = self._run_remote_command(self.follower_host, backup_logs_cmd)
            if backup_logs_result.returncode != 0:
                logger.warning(f"Failed to backup Ratis logs: {backup_logs_result.stderr}")
        
        # Move original logs to safe location
        move_logs_cmd = f"mv {current_dir}/log* {self.backup_dir}/ratisLogs_{self.epoch_time}/original/ 2>/dev/null || true"
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would run: {move_logs_cmd}")
        else:
            move_logs_result = self._run_remote_command(self.follower_host, move_logs_cmd)
            if move_logs_result.returncode != 0:
                logger.warning(f"Failed to move Ratis logs: {move_logs_result.stderr}")
        
        logger.info(f"[>] Ratis logs backed up to {self.backup_dir}/ratisLogs_{self.epoch_time}")
        return True
    
    def start_follower(self) -> bool:
        """Start the follower OM"""
        # logger.info(f"[7.2] Starting follower OM on {self.follower_host}...")
        
        if not self.follower_role:
            logger.error("Follower role not identified")
            return False
        
        role_name = self.follower_role.get("name")
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would start role: {role_name}")
            return True
        
        try:
            result = self.cm_client.role_command(
                self.cluster_name, 
                self.ozone_service, 
                "start", 
                [role_name]
            )
            logger.info(f"[>] Start command initiated: {result}")
            
            # Wait for the process to start by checking for the OzoneManagerStarter process
            logger.info(f"[>] Waiting for OzoneManagerStarter process to start...")
            max_wait_time = 180  # 3 minutes max wait
            wait_interval = 10   # Check every 10 seconds
            elapsed_time = 0
            
            while elapsed_time < max_wait_time:
                # Check if the OzoneManagerStarter process is running
                check_cmd = f"pgrep -f 'org.apache.hadoop.ozone.om.OzoneManagerStarter'"
                check_result = self._run_remote_command(self.follower_host, check_cmd)
                
                if check_result.returncode == 0:
                    # Process found, it has started
                    logger.info(f"[>] OzoneManagerStarter process started successfully (after {elapsed_time} seconds)")
                    self.om_role_was_stopped = False
                    return True
                
                # Process not running yet, wait and check again
                time.sleep(wait_interval)
                elapsed_time += wait_interval
                logger.info(f"[>] Process not running yet, waiting... ({elapsed_time}/{max_wait_time} seconds)")
            
            # If we reach here, the process didn't start within the timeout
            logger.error(f"OzoneManagerStarter process did not start within {max_wait_time} seconds")
            return False
            
        except Exception as e:
            logger.error(f"Failed to start follower OM: {e}")
            return False
    
    def verify_om_status(self) -> bool:
        """Verify OM status and test leadership transfer"""
        logger.info("[7.0] Verifying OM status...")
        
        # Wait a bit more for OM to fully join the cluster
        time.sleep(30)
        
        # Get updated roles
        if not self._get_om_roles_from_cli():
            logger.error("Failed to get updated OM roles")
            return False
        return True

    def test_leadership_transfer(self) -> bool:
        # Test leadership transfer
        logger.info("[8.0] Testing leadership transfer...")
        
        # Get node ID for the bootstrapped follower
        node_id_cmd = f"ozone admin om getserviceroles --service-id={self.service_id}"
        node_result = self._run_remote_command(self.follower_host, node_id_cmd)
        
        if node_result.returncode == 0:
            # Parse the output to find the node ID for our follower
            output = node_result.stdout
            logger.info(f"OM roles output:\n{output}")
            
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
                logger.info(f"[>] Testing leadership transfer to node ID: {node_id}")
            else:
                logger.error(f"Could not extract node ID for follower {self.follower_host}")
                logger.info(f"Available lines: {lines}")
                return False
        else:
            logger.error(f"Failed to get service roles: {node_result.stderr}")
            return False
        
        # Now proceed with the transfer
        transfer_cmd = f"ozone admin om transfer -id={self.service_id} -n {node_id}"
        transfer_result = self._run_remote_command(self.follower_host, transfer_cmd)
        
        if transfer_result.returncode == 0:
            logger.info(f"[>] Leadership transfer test successful")
            
            # Wait for transfer to complete
            time.sleep(30)
            
            # Verify the transfer
            if self._get_om_roles_from_cli():
                if self.leader_host == self.follower_host:
                    logger.info(f"[>] Leadership transfer confirmed: {self.follower_host} is now leader")
                else:
                    logger.info(f"[>] Leadership transfer test completed, {self.follower_host} is follower")
            return True
        else:
            logger.warning(f"Leadership transfer test failed: {transfer_result.stderr}")
            return False
    
    def cleanup(self):
        """Clean up temporary files"""
        if self.temp_dir:
            # Clean up remote temporary directory
            cleanup_cmd = f"rm -rf {self.temp_dir}"
            cleanup_result = self._run_remote_command(self.follower_host, cleanup_cmd)
            
            if cleanup_result.returncode == 0:
                logger.info(f"[>] Cleaned up remote temporary directory: {self.temp_dir}")
            else:
                logger.warning(f"Failed to clean up remote temporary directory {self.temp_dir}: {cleanup_result.stderr}")
    
    def _run_remote_command(self, host: str, command: str, use_sudo: bool = False) -> subprocess.CompletedProcess:
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
        
        # Add sudo if requested and sudo user is configured (but not when SSH user is root)
        if use_sudo and self.sudo_user and self.ssh_user != "root":
            final_cmd = f"sudo -u {self.sudo_user} {kinit_cmd}"
        else:
            final_cmd = kinit_cmd
        
        ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no"]
        
        # Add SSH user if specified
        if self.ssh_user:
            ssh_cmd.extend(["-l", self.ssh_user])
        
        ssh_cmd.extend([host, final_cmd])
        
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
            logger.info(f"[>] Using CM host for ozone commands: {command_host}")
        except Exception as e:
            logger.error(f"Failed to extract CM host from URL {self.cm_client.base_url}: {e}")
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
        logger.info("[1.5] Checking Ozone security configuration...")
        
        if not self.ozone_service:
            logger.error("Ozone service not discovered yet")
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
                    logger.info(f"[>] Ozone security enabled: {security_enabled}")
                    break
            
            if self.ozone_security_enabled:
                logger.info("[>] Kerberos authentication required for Ozone operations")
                
                # Check if keytab and principal are provided
                if not self.keytab or not self.principal:
                    logger.error("Ozone security is enabled but Kerberos credentials are missing")
                    logger.info("Please provide --keytab and --principal options")
                    return False
            
            # Check HTTP Kerberos configuration (this is separate from ozone.security.enabled)
            if self.ozone_http_kerberos_enabled:
                logger.info("[>] HTTP Kerberos authentication required for checkpoint download")
                
                # Check if keytab and principal are provided
                if not self.keytab or not self.principal:
                    logger.error("HTTP Kerberos is enabled but Kerberos credentials are missing")
                    logger.info("Please provide --keytab and --principal options")
                    return False
                
                # Use CM host for keytab validation since ozone commands run there
                try:
                    cm_host = self.cm_client.base_url.split("://")[1].split(":")[0]
                except Exception as e:
                    logger.error(f"Failed to extract CM host from URL {self.cm_client.base_url}: {e}")
                    return False
                
                # Validate keytab file exists on CM host
                keytab_check_cmd = f"test -f {self.keytab} && echo 'EXISTS' || echo 'NOT_FOUND'"
                result = self._run_remote_command(cm_host, keytab_check_cmd)
                
                if result.returncode != 0 or 'NOT_FOUND' in result.stdout:
                    logger.error(f"Keytab file not found on CM host {cm_host}: {self.keytab}")
                    return False
                
                logger.info(f"[>] Using keytab: {self.keytab}")
                logger.info(f"[>] Using principal: {self.principal}")
                
                # Test kinit on CM host
                if not self.dry_run:
                    kinit_cmd = f"kinit -kt {self.keytab} {self.principal}"
                    result = self._run_remote_command(cm_host, kinit_cmd)
                    
                    if result.returncode != 0:
                        logger.error(f"Failed to authenticate with Kerberos: {result.stderr}")
                        return False
                    
                    logger.info("[>] Kerberos authentication successful")
            else:
                logger.info("[>] Ozone security is disabled - no Kerberos authentication required")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to check security configuration: {e}")
            return False
    
    def run_bootstrap(self) -> bool:
        """Run the complete bootstrap process"""
        try:
            logger.info("=" * 80)
            logger.info("OZONE MANAGER BOOTSTRAP AUTOMATION")
            logger.info("=" * 80)
            
            # Step 1.0: Find healthy leader
            if not self.discover_cluster_info():
                return False
            
            # Step 1.1: Validate SSH connectivity and sudo access
            if not self.validate_ssh_connectivity():
                return False
            
            if not self.validate_sudo_access():
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
            
            # Step 1.5: Check security configuration (do this before any ozone commands)
            if not self.check_security_configuration():
                return False
            
            # Step 2.0: Check OM leader health
            if not self.verify_leader_health():
                return False
            
            # Step 2.1: List last Ratis log files before bootstrapping
            logger.info("[2.1] Listing Ratis log files before bootstrapping...")
            
            # Debug: Check if leader and follower are the same
            if self.leader_host == self.follower_host:
                logger.info(f"[DEBUG] Leader and follower are the same host: {self.leader_host}")
                # Only list once if they're the same
                if not self.list_last_ratis_log("before", self.leader_host):
                    logger.warning("Failed to list Ratis log file before bootstrapping")
            else:
                # List leader Ratis log
                if not self.list_last_ratis_log("before", self.leader_host):
                    logger.warning("Failed to list leader Ratis log file before bootstrapping")
                
                # List follower Ratis log
                if not self.list_last_ratis_log("before", self.follower_host):
                    logger.warning("Failed to list follower Ratis log file before bootstrapping")
            
            # Step 3.0: Stop follower
            logger.info(f"[3.0] Stopping follower OM on {self.follower_host}...")
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
            logger.info(f"[6.0] Starting follower OM on {self.follower_host}...")
            if not self.start_follower():
                return False
            
            # Step 7.0: Verify status
            if not self.verify_om_status():
                return False
            
            # Step 7.1: List last Ratis log files after bootstrapping
            logger.info("[7.1] Listing Ratis log files after bootstrapping...")
            
            # Debug: Check if leader and follower are the same
            if self.leader_host == self.follower_host:
                logger.info(f"[DEBUG] Leader and follower are the same host: {self.leader_host}")
                # Only list once if they're the same
                if not self.list_last_ratis_log("after", self.leader_host):
                    logger.warning("Failed to list Ratis log file after bootstrapping")
            else:
                # List leader Ratis log
                if not self.list_last_ratis_log("after", self.leader_host):
                    logger.warning("Failed to list leader Ratis log file after bootstrapping")
                
                # List follower Ratis log
                if not self.list_last_ratis_log("after", self.follower_host):
                    logger.warning("Failed to list follower Ratis log file after bootstrapping")
            
            # Step 7.2: Restart OM role if it was stopped during bootstrap
            if self.om_role_was_stopped:
                logger.info("[7.2] Restarting OM role that was stopped during bootstrap...")
                if not self.start_follower():
                    logger.warning("Failed to restart OM role")
                else:
                    logger.info("[>] OM role restarted successfully")

            # Step 8.0: Test leadership transfer (optional)
            if self.enable_leadership_transfer_test:
                if not self.test_leadership_transfer():
                    return False
            
            logger.info("=" * 80)
            logger.info("BOOTSTRAP PROCESS COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            return True
            
        except Exception as e:
            logger.error(f"Bootstrap process failed: {e}")
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
  ./ozone_om_bootstrap.py --cm-base-url https://<cm>:7183 --list-clusters --insecure

  # Run bootstrap with dry-run to see what would happen (unsecured cluster, root SSH user)
  ./ozone_om_bootstrap.py --cm-base-url https://<cm>:7183 --cluster "Cluster 1" \\
    --follower-host om-node-2.example.com --insecure --dry-run

  # Run bootstrap with dry-run (secured cluster with Kerberos, root SSH user)
  ./ozone_om_bootstrap.py --cm-base-url https://<cm>:7183 --cluster "Cluster 1" \\
    --follower-host om-node-2.example.com --keytab /etc/security/keytabs/om.keytab --principal om/om-node-2.example.com@REALM \\
    --insecure --dry-run

  # Run bootstrap with dry-run (non-root SSH user, requires sudo user)
  ./ozone_om_bootstrap.py --cm-base-url https://<cm>:7183 --cluster "Cluster 1" \\
    --follower-host om-node-2.example.com --ssh-user admin --sudo-user hdfs --insecure --dry-run

  # Run actual bootstrap (unsecured cluster, root SSH user)
  ./ozone_om_bootstrap.py --cm-base-url https://<cm>:7183 --cluster "Cluster 1" \\
    --follower-host om-node-2.example.com --insecure --yes

  # Run actual bootstrap (secured cluster with Kerberos, root SSH user)
  ./ozone_om_bootstrap.py --cm-base-url https://<cm>:7183 --cluster "Cluster 1" \\
    --follower-host om-node-2.example.com --keytab /etc/security/keytabs/om.keytab --principal om/om-node-2.example.com@REALM \\
    --insecure --yes
        """
    )
    
    parser.add_argument("--cm-base-url", required=True,
                       help="Cloudera Manager base URL (e.g., https://<cm>:7183)")
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
    parser.add_argument("--ssh-user", default="root",
                       help="SSH user for connecting to remote hosts (default: root)")
    parser.add_argument("--sudo-user",
                       help="Sudo user for running privileged commands (not needed when SSH user is root)")
    parser.add_argument("--enable-leadership-transfer-test", action="store_true",
                       help="Run step [8.0] to test leadership transfer after verifying status")
    parser.add_argument("--verbose", action="store_true",
                       help="Enable verbose (DEBUG) logging")
    
    args = parser.parse_args()

    # Initialize logging
    _configure_logging()
    if args.verbose or args.dry_run:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
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
        logger.error("--cluster is required for bootstrap operations")
        logger.error("Use --list-clusters to see available clusters")
        sys.exit(1)
    
    if not args.follower_host:
        logger.error("--follower-host is required for bootstrap operations")
        sys.exit(1)
    
    # Validate arguments
    if not args.dry_run and not args.yes:
        logger.error("--yes is required for non-dry-run execution")
        sys.exit(1)
    
    # Create bootstrap instance
    bootstrap = OzoneOMBootstrap(
        cm_client=cm_client,
        cluster_name=args.cluster,
        follower_host=args.follower_host,
        insecure=args.insecure,
        dry_run=args.dry_run,
        keytab=args.keytab,
        principal=args.principal,
        ssh_user=args.ssh_user,
        sudo_user=args.sudo_user,
        enable_leadership_transfer_test=args.enable_leadership_transfer_test
    )
    
    # Run bootstrap
    success = bootstrap.run_bootstrap()
    
    if success:
        logger.info("Bootstrap completed successfully!")
        sys.exit(0)
    else:
        logger.error("Bootstrap failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
