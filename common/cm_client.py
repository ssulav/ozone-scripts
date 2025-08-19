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
Common Cloudera Manager (CM) API client utilities for cluster-operations scripts.

Features:
- Basic GET/POST helpers with auth and SSL verification controls
- List clusters/services/roles
- Fetch role configuration
- Fetch host by id
- Service-level commands (start/stop)
- Role-level commands (start/stop) via roleCommands endpoints
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union
import time
import logging

from urllib.parse import quote

# Suppress SSL warnings
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

try:
    import requests  # type: ignore
except Exception as exc:  # pragma: no cover - graceful fallback message
    raise RuntimeError("This module requires the 'requests' package. Install with: pip install requests") from exc


@dataclass
class CmClient:
    base_url: str
    api_version: str
    username: str
    password: str
    verify: Union[bool, str] = True  # requests verify can be bool or CA bundle path

    def _url(self, path: str) -> str:
        if self.base_url.endswith('/'):
            return f"{self.base_url}api/{self.api_version}{path}"
        return f"{self.base_url}/api/{self.api_version}{path}"

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None, json: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = self._url(path)
        response = requests.request(
            method=method,
            url=url,
            params=params or {},
            json=json,
            auth=(self.username, self.password),
            timeout=60,
            verify=self.verify,
        )
        try:
            response.raise_for_status()
        except Exception as exc:  # pragma: no cover - propagate with context
            msg = f"HTTP {response.status_code} for {method} {url}: {response.text}"
            raise RuntimeError(msg) from exc
        try:
            return response.json()
        except Exception:
            return {}

    # Convenience wrappers
    def list_clusters(self) -> List[Dict[str, Any]]:
        data = self._request("GET", "/clusters")
        return data.get("items", [])

    def list_services(self, cluster_name: str) -> List[Dict[str, Any]]:
        path = f"/clusters/{quote(cluster_name, safe='')}" + "/services"
        data = self._request("GET", path)
        return data.get("items", [])

    def list_roles(self, cluster_name: str, service_name: str) -> List[Dict[str, Any]]:
        path = f"/clusters/{quote(cluster_name, safe='')}/services/{quote(service_name, safe='')}" + "/roles"
        data = self._request("GET", path)
        return data.get("items", [])

    def get_role_config(self, cluster_name: str, service_name: str, role_name: str, view: str = "FULL") -> Dict[str, Any]:
        path = (
            f"/clusters/{quote(cluster_name, safe='')}/services/{quote(service_name, safe='')}"
            f"/roles/{quote(role_name, safe='')}/config"
        )
        params = {"view": view}
        return self._request("GET", path, params=params)

    def get_service_config(self, cluster_name: str, service_name: str, view: str = "FULL") -> Dict[str, Any]:
        """Get service-level configuration"""
        path = f"/clusters/{quote(cluster_name, safe='')}/services/{quote(service_name, safe='')}/config"
        params = {"view": view}
        return self._request("GET", path, params=params)

    def get_host_by_id(self, host_id: str) -> Dict[str, Any]:
        path = f"/hosts/{quote(host_id, safe='')}"
        return self._request("GET", path)

    def service_command(self, cluster_name: str, service_name: str, command: str) -> Dict[str, Any]:
        path = f"/clusters/{quote(cluster_name, safe='')}/services/{quote(service_name, safe='')}/commands/{command}"
        # CM expects application/json; send an empty JSON body for command POSTs
        return self._request("POST", path, json={})

    def role_command(self, cluster_name: str, service_name: str, command: str, role_names: List[str]) -> Dict[str, Any]:
        """Issue a role-level command (e.g., start/stop) with payload {"items": [roleName, ...]}"""
        path = f"/clusters/{quote(cluster_name, safe='')}/services/{quote(service_name, safe='')}/roleCommands/{command}"
        payload = {"items": role_names}
        return self._request("POST", path, json=payload)

    # Configuration helpers
    def update_service_config(self, cluster_name: str, service_name: str, items: Dict[str, Any]) -> Dict[str, Any]:
        """Update service-level configuration with a dict of name->value."""
        path = f"/clusters/{quote(cluster_name, safe='')}/services/{quote(service_name, safe='')}/config"
        payload = {"items": [{"name": k, "value": v} for k, v in items.items()]}
        return self._request("PUT", path, json=payload)

    def list_role_config_groups(self, cluster_name: str, service_name: str) -> List[Dict[str, Any]]:
        path = f"/clusters/{quote(cluster_name, safe='')}/services/{quote(service_name, safe='')}/roleConfigGroups"
        data = self._request("GET", path)
        return data.get("items", [])

    def update_role_config_group(self, cluster_name: str, service_name: str, role_config_group_name: str, items: Dict[str, Any]) -> Dict[str, Any]:
        """Update configuration for a specific role config group by name."""
        path = (
            f"/clusters/{quote(cluster_name, safe='')}/services/{quote(service_name, safe='')}"
            f"/roleConfigGroups/{quote(role_config_group_name, safe='')}/config"
        )
        payload = {"items": [{"name": k, "value": v} for k, v in items.items()]}
        return self._request("PUT", path, json=payload)

    def update_role_type_config(self, cluster_name: str, service_name: str, role_type: str, items: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Update config for all role config groups matching role_type (e.g., 'OZONE_MANAGER')."""
        role_type_upper = role_type.upper()
        groups = self.list_role_config_groups(cluster_name, service_name)
        results: List[Dict[str, Any]] = []
        for g in groups:
            g_type = (g.get("roleType") or g.get("roleTypeName") or "").upper()
            g_name = g.get("name")
            if g_type == role_type_upper and g_name:
                res = self.update_role_config_group(cluster_name, service_name, g_name, items)
                results.append(res)
        return results

    def get_role_config_group_config(self, cluster_name: str, service_name: str, role_config_group_name: str, view: str = "FULL") -> Dict[str, Any]:
        path = (
            f"/clusters/{quote(cluster_name, safe='')}/services/{quote(service_name, safe='')}"
            f"/roleConfigGroups/{quote(role_config_group_name, safe='')}/config"
        )
        return self._request("GET", path, params={"view": view})

    def find_safety_valve_key_for_role_group(self, cluster_name: str, service_name: str, role_config_group_name: str) -> Optional[str]:
        """Heuristically find the ozone-site.xml safety valve attribute name for a role config group."""
        cfg = self.get_role_config_group_config(cluster_name, service_name, role_config_group_name, view="FULL")
        items = cfg.get("items", [])
        candidates: List[str] = []
        for it in items:
            name = it.get("name") or ""
            lname = name.lower()
            if "safety" in lname and ("ozone" in lname or "ozone-site" in lname):
                candidates.append(name)
        # Prefer ones explicitly mentioning ozone-site
        for c in candidates:
            if "ozone-site" in c.lower():
                return c
        return candidates[0] if candidates else None

    def update_ozone_site_safety_valve_for_role_type(self, cluster_name: str, service_name: str, role_type: str, properties_xml: str) -> List[Dict[str, Any]]:
        """Append given properties_xml to ozone-site safety valve for all role config groups of role_type."""
        role_type_upper = role_type.upper()
        groups = self.list_role_config_groups(cluster_name, service_name)
        results: List[Dict[str, Any]] = []
        for g in groups:
            g_type = (g.get("roleType") or g.get("roleTypeName") or "").upper()
            g_name = g.get("name")
            if g_type != role_type_upper or not g_name:
                continue
            key = self.find_safety_valve_key_for_role_group(cluster_name, service_name, g_name)
            if not key:
                continue
            # Read current value
            cfg = self.get_role_config_group_config(cluster_name, service_name, g_name, view="FULL")
            items = cfg.get("items", [])
            current = ""
            for it in items:
                if it.get("name") == key:
                    current = (it.get("value") or it.get("effectiveValue") or it.get("displayValue") or it.get("default") or "")
                    break
            new_val = (current + "\n" if current and not current.endswith("\n") else current) + properties_xml
            res = self.update_role_config_group(cluster_name, service_name, g_name, {key: new_val})
            results.append(res)
        return results

    def ensure_ozone_site_properties_for_role_type(self, cluster_name: str, service_name: str, role_type: str, properties: Dict[str, str]) -> Dict[str, Any]:
        """Ensure each property (name->value) exists in ozone-site safety valve for groups of role_type.

        Only appends missing <property> entries; avoids duplicating existing ones.
        Returns a summary: {updated_groups: int, skipped_groups: int, details: [...]}
        """
        role_type_upper = role_type.upper()
        groups = self.list_role_config_groups(cluster_name, service_name)
        updated = 0
        skipped = 0
        details: List[Dict[str, Any]] = []
        for g in groups:
            g_type = (g.get("roleType") or g.get("roleTypeName") or "").upper()
            g_name = g.get("name")
            if g_type != role_type_upper or not g_name:
                continue
            key = self.find_safety_valve_key_for_role_group(cluster_name, service_name, g_name)
            if not key:
                details.append({"group": g_name, "action": "no_safety_valve"})
                continue
            cfg = self.get_role_config_group_config(cluster_name, service_name, g_name, view="FULL")
            items = cfg.get("items", [])
            current_val = ""
            for it in items:
                if it.get("name") == key:
                    current_val = (it.get("value") or it.get("effectiveValue") or it.get("displayValue") or it.get("default") or "")
                    break
            missing_xml_parts: List[str] = []
            for pname, pval in properties.items():
                if pname in current_val:
                    continue
                missing_xml_parts.append(f"<property><name>{pname}</name><value>{pval}</value></property>")
            if not missing_xml_parts:
                skipped += 1
                details.append({"group": g_name, "action": "skipped"})
                continue
            add_xml = "".join(missing_xml_parts)
            new_val = current_val + ("\n" if current_val and not current_val.endswith("\n") else "") + add_xml
            res = self.update_role_config_group(cluster_name, service_name, g_name, {key: new_val})
            details.append({"group": g_name, "action": "updated", "response": res})
            updated += 1
        return {"updated_groups": updated, "skipped_groups": skipped, "details": details}

    # Command polling
    def get_command(self, command_id: Union[int, str]) -> Dict[str, Any]:
        path = f"/commands/{quote(str(command_id))}"
        return self._request("GET", path)

    def wait_for_command(self, command_id: Union[int, str], timeout_sec: int = 900, interval_sec: int = 5) -> Dict[str, Any]:
        deadline = time.time() + timeout_sec
        last = {}
        while time.time() < deadline:
            cmd = self.get_command(command_id)
            last = cmd
            if not cmd.get("active", False):
                return cmd
            time.sleep(interval_sec)
        return last

    # High-level service helpers
    def stop_service_and_wait(self, cluster_name: str, service_name: str, timeout_sec: int = 1800) -> Dict[str, Any]:
        resp = self.service_command(cluster_name, service_name, "stop")
        cmd_id = resp.get("id")
        if cmd_id is None:
            return resp
        return self.wait_for_command(cmd_id, timeout_sec=timeout_sec)

    def start_service_and_wait(self, cluster_name: str, service_name: str, timeout_sec: int = 1800) -> Dict[str, Any]:
        resp = self.service_command(cluster_name, service_name, "start")
        cmd_id = resp.get("id")
        if cmd_id is None:
            return resp
        return self.wait_for_command(cmd_id, timeout_sec=timeout_sec)

    def deploy_client_config_and_wait(self, cluster_name: str, service_name: str, timeout_sec: int = 1800) -> Dict[str, Any]:
        resp = self.service_command(cluster_name, service_name, "deployClientConfig")
        cmd_id = resp.get("id")
        if cmd_id is None:
            return resp
        return self.wait_for_command(cmd_id, timeout_sec=timeout_sec)

    # Higher-level helpers (fetch + mapping)
    def get_host_map(self) -> Dict[str, str]:
        """Returns mapping of hostId -> hostname"""
        hosts = self._request("GET", "/hosts").get("items", [])
        return {h.get("hostId"): h.get("hostname") or h.get("hostId") for h in hosts}

    def resolve_service_name(self, cluster_name: str, service_hint: str) -> str:
        """Resolve a friendly hint like 'ozone' to a concrete service name (e.g., OZONE-1)."""
        if not service_hint or service_hint.lower() != "ozone":
            return service_hint
        services = self.list_services(cluster_name)
        for s in services:
            name = s.get("name") or ""
            if name.upper().startswith("OZONE"):
                return name
        # fallback to original hint if nothing found
        return service_hint

    def list_roles_with_hostnames(self, cluster_name: str, service_name: str) -> List[Dict[str, Any]]:
        """List roles augmented with 'hostname' resolved from hostRef.hostId."""
        roles = self.list_roles(cluster_name, service_name)
        host_map = self.get_host_map()
        for r in roles:
            hid = (r.get("hostRef") or {}).get("hostId")
            r["hostname"] = host_map.get(hid, hid)
        return roles

    def discover_clusters(self) -> List[Dict[str, Any]]:
        """Discover and display available clusters in Cloudera Manager"""
        logger = logging.getLogger("ozone.cm_client")
        logger.info(f"Connecting to Cloudera Manager at: {self.base_url}")
        logger.info("-" * 60)
        
        try:
            clusters = self.list_clusters()
            
            if not clusters:
                logger.info("No clusters found in Cloudera Manager.")
                return clusters
            
            logger.info(f"Found {len(clusters)} cluster(s):")
            logger.info("")
            
            for i, cluster in enumerate(clusters, 1):
                cluster_name = cluster.get('name', 'Unknown')
                cluster_display_name = cluster.get('displayName', cluster_name)
                cluster_version = cluster.get('version', 'Unknown')
                cluster_status = cluster.get('entityStatus', 'Unknown')
                
                logger.info(f"{i}. Cluster Name: '{cluster_name}'")
                logger.info(f"   Display Name: {cluster_display_name}")
                logger.info(f"   Version: {cluster_version}")
                logger.info(f"   Status: {cluster_status}")
                logger.info("")
            
            logger.info("Use the exact 'Cluster Name' value in the bootstrap script.")
            if clusters:
                logger.info("Example:")
                logger.info(f"  --cluster '{clusters[0]['name']}'")
            
            return clusters
            
        except Exception as e:
            logger = logging.getLogger("ozone.cm_client")
            logger.error(f"Error discovering clusters: {e}")
            return []

    def print_cluster_info(self, cluster_name: str) -> bool:
        """Print detailed information about a specific cluster"""
        logger = logging.getLogger("ozone.cm_client")
        try:
            clusters = self.list_clusters()
            target_cluster = None
            
            for cluster in clusters:
                if cluster.get('name') == cluster_name:
                    target_cluster = cluster
                    break
            
            if not target_cluster:
                logger.error(f"Cluster '{cluster_name}' not found.")
                logger.info("Available clusters:")
                for cluster in clusters:
                    logger.info(f"  - '{cluster.get('name')}'")
                return False
            
            logger.info(f"Cluster Information for '{cluster_name}':")
            logger.info("-" * 50)
            logger.info(f"Name: {target_cluster.get('name')}")
            logger.info(f"Display Name: {target_cluster.get('displayName')}")
            logger.info(f"Version: {target_cluster.get('version')}")
            logger.info(f"Status: {target_cluster.get('entityStatus')}")
            logger.info(f"CDH Version: {target_cluster.get('cdhVersion')}")
            logger.info(f"Full Version: {target_cluster.get('fullVersion')}")
            
            # List services
            services = self.list_services(cluster_name)
            logger.info(f"\nServices ({len(services)}):")
            for service in services:
                service_name = service.get('name', 'Unknown')
                service_type = service.get('type', 'Unknown')
                service_status = service.get('serviceState', 'Unknown')
                logger.info(f"  - {service_name} ({service_type}): {service_status}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error getting cluster info: {e}")
            return False


