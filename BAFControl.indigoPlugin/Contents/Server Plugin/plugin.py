#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
Indigo Plugin for Big Ass Fans (BAF) i6/Haiku devices.

This plugin bridges Indigo's synchronous Python environment with the
asynchronous aiobafi6 library using a background asyncio event loop.
It supports separate Indigo devices via a bridge device for fan motor
and light control, full status synchronization, and robust error handling
with retries.
"""

import asyncio
import socket
import threading
import indigo
from aiobafi6 import Device
from zeroconf.asyncio import AsyncServiceBrowser, AsyncZeroconf

# Indigo brightness is 0-100. BAF fan speed is 0-7 (approx 14% each step)
SPEED_SCALE = 100.0 / 7.0
# BAF light brightness is 0-16 (approx 6.25% each step)
BRIGHTNESS_SCALE = 100.0 / 16.0

# Global dictionary to store verified devices from mDNS: {Service_Name: {address, display}}
DISCOVERED_FANS = {}


class Plugin(indigo.PluginBase):
    """
    Main Indigo Plugin class responsible for managing BAF devices.
    """
    def __init__(self, pluginId, pluginDisplayName, pluginVersion, pluginPrefs): # pylint: disable=invalid-name
        """Initialize plugin, data structures, and the async thread."""
        super().__init__(pluginId, pluginDisplayName, pluginVersion, pluginPrefs)
        # {BRIDGE_DEV_ID: Device_Instance}
        self.active_connections = {}
        # {BRIDGE_DEV_ID: [list_of_child_indigo_device_ids]}
        self.bridge_to_children_map = {}
        # {BRIDGE_DEV_ID: Task_Instance}
        self.reconnect_tasks = {}

        self.azc = None
        self.browser = None

        # Initialize background asyncio loop
        self.loop = asyncio.new_event_loop()
        self.async_thread = threading.Thread(target=self._run_async_loop, daemon=True)
        self.async_thread.start()
        self.debugLog("Asyncio background thread initialized.")

    def _run_async_loop(self):
        """Internal method to start and maintain the asyncio loop indefinitely."""
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def _validate_address(self, address):
        """Checks if a hostname or IP address is reachable via DNS."""
        try:
            socket.gethostbyname(address)
            return True
        except socket.gaierror:
            return False

    def deviceStartComm(self, dev): # pylint: disable=invalid-name
        """Called by Indigo when a device is enabled."""
        if dev.deviceTypeId == "bafBridge":
            # Determine address from UI (Manual vs Discovery)
            ip_address = dev.pluginProps.get("selected_address_source")
            if ip_address == "manual":
                ip_address = dev.pluginProps.get("manual_address")
            if not ip_address:
                return

            # Validate address resolution
            if not ip_address or not self._validate_address(ip_address):
                dev.setErrorStateOnServer("invalid address")
                self.errorLog(f"Device '{dev.name}' has an unresolvable address: {ip_address}")
                return

            # Cache the resolved address
            if dev.pluginProps.get("address") != ip_address:
                props = dev.pluginProps
                props["address"] = ip_address
                dev.replacePluginPropsOnServer(props)

            self.infoLog(f"Starting bridge communication for '{dev.name}' at {ip_address}")

            # Start connection supervisor
            if dev.id not in self.reconnect_tasks:
                self.reconnect_tasks[dev.id] = asyncio.run_coroutine_threadsafe(
                    self._connection_supervisor(dev.id, ip_address), self.loop
                )

            # Start mDNS Browser once
            if self.azc is None:
                asyncio.run_coroutine_threadsafe(self._start_discovery_service(), self.loop)

        elif dev.deviceTypeId in ("bafFan", "bafLight"):
            bridge_id_str = dev.pluginProps.get("bridge_id")
            if bridge_id_str:
                bridge_id = int(bridge_id_str)
                self.bridge_to_children_map.setdefault(bridge_id, []).append(dev.id)

    async def _connection_supervisor(self, bridge_id, ip_address):
        """
        Manages hardware connection lifecycle with exponential backoff.
        This task runs forever in the background loop.
        """
        backoff = 5.0
        max_backoff = 300.0

        while True:
            try:
                self.debugLog(f"Supervisor: Attempting connection to bridge ID {bridge_id} ({ip_address})") # pylint: disable=line-too-long
                baf = Device(ip_address)

                def state_callback(device):
                    # Update Bridge visibility state based on hardware discovery
                    bridge_dev = indigo.devices.get(bridge_id)
                    if bridge_dev:
                        bridge_dev.updateStateOnServer("has_light_hardware", device.has_light)

                    # Update children
                    for child_id in self.bridge_to_children_map.get(bridge_id, []):
                        child_dev = indigo.devices.get(child_id)
                        if child_dev:
                            self._update_indigo_states(child_dev, device)

                baf.add_callback(state_callback)
                self.active_connections[bridge_id] = baf

                # Clear error states in Indigo on successful start/reconnect
                indigo.devices[bridge_id].setErrorStateOnServer(None)

                # Wait for initial data and maintain connection until it drops
                await baf.async_run()
                raise Exception("Connection closed") # pylint: disable=broad-exception-raised

            except Exception as e: # pylint: disable=broad-exception-caught
                self.active_connections.pop(bridge_id, None)
                self.errorLog(f"BAF Connection Error (Bridge ID {bridge_id}): {str(e)}")
                indigo.devices[bridge_id].setErrorStateOnServer("offline")

                self.warnLog(f"Reconnecting to Bridge ID {bridge_id} in {backoff} seconds...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    def _get_baf_device_from_child(self, dev):
        """Helper function to find the parent bridge's Device instance."""
        bridge_id_str = dev.pluginProps.get("bridge_id")
        if not bridge_id_str:
            return None
        return self.active_connections.get(int(bridge_id_str))

    def _update_indigo_states(self, dev, baf):
        """Routes BAF hardware properties to specific Indigo device types."""
        if self.pluginPrefs.get("showDebugLogging", False):
            self.debugLog(f"Updating states for {dev.name}")

        try:
            if dev.deviceTypeId == "bafFan":
                dev.updateStatesOnServer([
                    {'key': 'speed', 'value': baf.speed},
                    {'key': 'onOffState', 'value': baf.fan_on},
                    {'key': 'auto_mode', 'value': baf.fan_auto_on},
                    {'key': 'whoosh_mode', 'value': baf.whoosh_mode_on},
                    {'key': 'eco_mode', 'value': baf.eco_mode_on},
                    {'key': 'reverse', 'value': baf.reverse_direction_on}
                ])
            elif dev.deviceTypeId == "bafLight" and baf.has_light:
                dev.updateStatesOnServer([
                    {'key': 'onOffState', 'value': baf.light_on},
                    {'key': 'brightness',
                     'value': (baf.light_brightness * BRIGHTNESS_SCALE)},
                    {'key': 'warmth', 'value': baf.light_warmth},
                    {'key': 'auto_mode', 'value': baf.light_auto_on}
                ])
        except Exception as e: # pylint: disable=broad-exception-caught
            self.debugLog(f"State update failed for {dev.name}: {e}")


    # Discovery Logic (mDNS)
    async def _start_discovery_service(self):
        self.azc = AsyncZeroconf()
        self.browser = AsyncServiceBrowser(self.azc.zeroconf, "_api._tcp.local.", handlers=[self])

    def add_service(self, zc, type_, name):
        """Async callback for discovered services."""
        asyncio.run_coroutine_threadsafe(self._process_service(zc, type_, name), self.loop)

    async def _process_service(self, zc, type_, name):
        info = await zc.get_service_info(type_, name)
        if info and info.addresses:
            addr = socket.inet_ntoa(info.addresses)
            DISCOVERED_FANS[name] = {"address": addr, "display": f"BAF [{name}] ({addr})"}

    def remove_service(self, zc, type_, name): # pylint: disable=unused-argument
        """Handles device removal from discovery list."""
        DISCOVERED_FANS.pop(name, None)

    def update_service(self, zc, type_, name):
        """Handles property updates for discovered services."""
        asyncio.run_coroutine_threadsafe(self._process_service(zc, type_, name), self.loop)

    def getDiscoveredDevices(self, filter="", valuesDict=None, typeId="", targetId=0): # pylint: disable=invalid-name,unused-argument,redefined-builtin
        """Populates the ConfigUI with verified BAF devices."""
        items = [("manual", "Manual Input (Enter IP/Hostname below)")]
        for info in DISCOVERED_FANS.values():
            items.append((info['address'], info['display']))
        return items

    # --- Standard Indigo Action Callbacks ---
    def actionControlFan(self, action, dev): # pylint: disable=invalid-name
        """Handles standard Indigo Fan actions (On/Off/Speed)."""
        baf = self._get_baf_device_from_child(dev)
        if not baf:
            return

        self.infoLog(f"Sent Fan command {str(action.deviceAction)} to {dev.name}")

        if action.deviceAction == indigo.kDeviceAction.TurnOn:
            asyncio.run_coroutine_threadsafe(baf.async_set_fan_on(True), self.loop)
        elif action.deviceAction == indigo.kDeviceAction.TurnOff:
            asyncio.run_coroutine_threadsafe(baf.async_set_fan_on(False), self.loop)
        elif action.deviceAction == indigo.kDeviceAction.SetBrightness:
            speed = int(action.actionValue / SPEED_SCALE)
            asyncio.run_coroutine_threadsafe(baf.async_set_speed(speed), self.loop)

    def actionControlDimmerRelay(self, action, dev): # pylint: disable=invalid-name
        """Handles standard Indigo Light actions (On/Off/Brightness)."""
        baf = self._get_baf_device_from_child(dev)
        if not baf:
            return

        if not baf.has_light:
            self.warnLog(f"User attempted to control light for fan '{dev.name}' which is configured as lightless.") # pylint: disable=line-too-long
            return

        self.infoLog(f"Sent Light command {str(action.deviceAction)} to {dev.name}")

        if action.deviceAction == indigo.kDeviceAction.TurnOn:
            asyncio.run_coroutine_threadsafe(baf.async_set_light_on(True), self.loop)
        elif action.deviceAction == indigo.kDeviceAction.TurnOff:
            asyncio.run_coroutine_threadsafe(baf.async_set_light_on(False), self.loop)
        elif action.deviceAction == indigo.kDeviceAction.SetBrightness:
            brightness = int(action.actionValue / BRIGHTNESS_SCALE)
            asyncio.run_coroutine_threadsafe(baf.async_set_light_brightness(brightness), self.loop)

    def actionControlColorTemperature(self, action, dev): # pylint: disable=invalid-name
        """Handles standard Indigo color temperature action (Kelvin values)."""
        baf = self._get_baf_device_from_child(dev)
        if not baf:
            return

        bridge_id_str = dev.pluginProps.get("bridge_id")
        bridge_dev = indigo.devices[int(bridge_id_str)]
        if not bridge_dev.pluginProps.get("hasLight", True):
            self.warnLog(f"User attempted to control light temp for fan '{dev.name}' which is configured as lightless.") # pylint: disable=line-too-long
            return

        # Simple linear approximation map from Kelvin (2700-6500) to BAF 0-1000 range
        temp_k = action.actionValue
        baf_warmth = int(((temp_k - 2700) / (6500 - 2700)) * 1000)
        baf_warmth = max(0, min(1000, baf_warmth))  # Clamp range
        self.infoLog(f"Setting Light color temp {temp_k}K (BAF value: {baf_warmth}) on {dev.name}") # pylint: disable=line-too-long
        asyncio.run_coroutine_threadsafe(baf.async_set_light_warmth(baf_warmth), self.loop)

    def actionSetFanMode(self, action, dev): # pylint: disable=invalid-name
        """Handles custom menu actions for fan modes (Auto, Whoosh, Eco, Reverse)."""
        baf = self._get_baf_device_from_child(dev)
        if not baf:
            return

        mode = action.props.get("modeType")
        val = action.props.get("value").lower() == "true"
        self.infoLog(f"Setting Fan mode '{mode}' to {val} on {dev.name}")

        dispatch = {
            "auto_mode": baf.async_set_fan_auto_on,
            "whoosh_mode": baf.async_set_whoosh_mode_on,
            "eco_mode": baf.async_set_eco_mode_on,
            "reverse": baf.async_set_reverse_direction_on
        }
        if mode in dispatch:
            asyncio.run_coroutine_threadsafe(dispatch[mode](val), self.loop)

    def actionSetLightMode(self, action, dev): # pylint: disable=invalid-name
        """Handles custom menu actions for light modes (currently only Auto Mode)."""
        baf = self._get_baf_device_from_child(dev)
        if not baf:
            return

        if action.props.get("modeType") == "auto_mode":
            val = action.props.get("value") == "True"
            self.infoLog(f"Setting Light auto_mode to {val} on {dev.name}")
            asyncio.run_coroutine_threadsafe(baf.async_set_light_auto_on(val), self.loop)

    def deviceStopComm(self, dev): # pylint: disable=invalid-name
        """Called by Indigo when a device is disabled, handles cleanup."""
        if dev.deviceTypeId == "bafBridge":
            # Clean up bridge-specific resources when bridge is stopped
            self.infoLog(f"Stopping bridge communication for {dev.name}")
            self.bridge_to_children_map.pop(dev.id, None)
            task = self.reconnect_tasks.pop(dev.id, None)
            if task:
                task.cancel()
            baf = self.active_connections.pop(dev.id, None)
            if baf:
                self.loop.call_soon_threadsafe(baf.async_stop)

            # Also stop discovery services
            if self.browser:
                asyncio.run_coroutine_threadsafe(self.browser.async_cancel(), self.loop)
                self.browser = None
            if self.azc:
                asyncio.run_coroutine_threadsafe(self.azc.async_close(), self.loop)
                self.azc = None

        elif dev.deviceTypeId in ("bafFan", "bafLight"):
            # Remove child device from mapping
            bridge_id_str = dev.pluginProps.get("bridge_id")
            if bridge_id_str:
                bridge_id = int(bridge_id_str)
                if bridge_id in self.bridge_to_children_map:
                    if dev.id in self.bridge_to_children_map[bridge_id]:
                        self.bridge_to_children_map[bridge_id].remove(dev.id)

    def shutdown(self):
        """Called by Indigo when the plugin is globally disabled."""
        self.infoLog("Plugin shutting down. Stopping background loop.")
        # Ensure discovery services are stopped on shutdown
        if self.browser:
            asyncio.run_coroutine_threadsafe(self.browser.async_cancel(), self.loop)
        if self.azc:
            asyncio.run_coroutine_threadsafe(self.azc.async_close(), self.loop)
        self.loop.stop()
