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
from aiobafi6 import BAFDevice
from zeroconf import ServiceListener, Zeroconf
from zeroconf.asyncio import AsyncServiceBrowser, AsyncZeroconf

# Define constants for clarity and to avoid Pylint 'magic number' warnings
# Indigo brightness is 0-100. BAF fan speed is 0-7 (approx 14% each step)
SPEED_SCALE = 100.0 / 7.0
# BAF light brightness is 0-16 (approx 6.25% each step)
BRIGHTNESS_SCALE = 100.0 / 16.0

# Global dictionary to store discovered devices for UI population
DISCOVERED_FANS = {}

class Plugin(indigo.PluginBase, ServiceListener):
    """
    Main Indigo Plugin class responsible for managing BAF devices.
    """
    def __init__(self, pluginId, pluginDisplayName, pluginVersion, pluginPrefs): # pylint: disable=invalid-name
        """Initialize plugin, data structures, and the async thread."""
        super().__init__(pluginId, pluginDisplayName, pluginVersion, pluginPrefs)
        # {BRIDGE_DEV_ID: BAFDevice_Instance}
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

    def deviceStartComm(self, dev): # pylint: disable=invalid-name
        """
        Called by Indigo when a device is enabled.
        If it's a bridge, starts supervisor; otherwise, registers child device.
        """
        if dev.deviceTypeId == "bafBridge":
            # Determine the IP address from the user's selection/input
            selected_source = dev.pluginProps.get("selected_address_source")
            if selected_source == "manual":
                ip_address = dev.pluginProps.get("manual_address")
            else:
                # If a discovered device was selected, use that IP as the source
                ip_address = selected_source

            if not ip_address:
                dev.setErrorStateOnServer("no ip configured")
                return

            # Store the final resolved address in the 'address' property for consistency
            if dev.pluginProps.get("address") != ip_address:
                dev.updateStateOnServer("address", ip_address)
                props = dev.pluginProps
                props["address"] = ip_address
                dev.replacePluginPropsOnServer(props)

            self.infoLog(f"Starting bridge communication for '{dev.name}' at {ip_address}")

            if dev.id not in self.reconnect_tasks:
                self.reconnect_tasks[dev.id] = asyncio.run_coroutine_threadsafe(
                    self._connection_supervisor(dev.id, ip_address), self.loop
                )
            # Initialize map entry for children
            if dev.id not in self.bridge_to_children_map:
                self.bridge_to_children_map[dev.id] = []

            # Start mDNS discovery service when the first bridge device starts
            if self.azc is None:
                asyncio.run_coroutine_threadsafe(self._start_discovery_service(), self.loop)

        elif dev.deviceTypeId in ("bafFan", "bafLight"):
            bridge_id_str = dev.pluginProps.get("bridge_id")
            if not bridge_id_str:
                return
            bridge_id = int(bridge_id_str)

            # Register child device ID with its parent bridge ID
            if bridge_id in self.bridge_to_children_map:
                if dev.id not in self.bridge_to_children_map[bridge_id]:
                    self.bridge_to_children_map[bridge_id].append(dev.id)
            self.infoLog(f"Child device '{dev.name}' linked to bridge ID {bridge_id}")

    async def _connection_supervisor(self, bridge_id, ip_address):
        """
        Manages hardware connection lifecycle with exponential backoff and logging.
        This task runs forever in the background loop.
        """
        backoff = 5.0
        max_backoff = 300.0

        while True:
            try:
                self.debugLog(f"Supervisor: Attempting connection to bridge ID {bridge_id} ({ip_address})") # pylint: disable=line-too-long
                baf = BAFDevice(ip_address)

                def state_callback(device):
                    """Callback triggered by aiobafi6 when device state changes."""
                    self.debugLog(f"Received update from BAF at {ip_address}")
                    for child_dev_id in self.bridge_to_children_map.get(bridge_id, []):
                        indigo_child_dev = indigo.devices.get(child_dev_id, None)
                        if indigo_child_dev:
                            self._update_indigo_states(indigo_child_dev, device)

                baf.add_callback(state_callback)
                self.active_connections[bridge_id] = baf

                # Clear error states in Indigo on successful start/reconnect
                indigo.devices[bridge_id].setErrorStateOnServer(None)

                # Wait for initial data and maintain connection until it drops
                await baf.async_run()

                # If async_run returns without exception, the connection was lost gracefully
                self.warnLog(f"BAF connection at {ip_address} was closed cleanly.")
                raise Exception("Connection closed") # pylint: disable=broad-exception-raised

            except Exception as e: # pylint: disable=broad-exception-caught
                self.active_connections.pop(bridge_id, None)
                self.errorLog(f"BAF Connection Error (Bridge ID {bridge_id}): {str(e)}")
                indigo.devices[bridge_id].setErrorStateOnServer("offline")

                self.warnLog(f"Reconnecting to Bridge ID {bridge_id} in {backoff} seconds...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    def _get_baf_device_from_child(self, dev):
        """Helper function to find the parent bridge's BAFDevice instance."""
        bridge_id_str = dev.pluginProps.get("bridge_id")
        if not bridge_id_str:
            self.warnLog(f"Device {dev.name} (ID {dev.id}) has no bridge_id set.")
            return None
        bridge_id = int(bridge_id_str)
        baf_device = self.active_connections.get(bridge_id)
        if not baf_device:
            self.warnLog(f"Bridge {bridge_id} for device {dev.name} is offline.")
        return baf_device

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
            elif dev.deviceTypeId == "bafLight":
                dev.updateStatesOnServer([
                    {'key': 'onOffState', 'value': baf.light_on},
                    {'key': 'brightness',
                     'value': (baf.light_brightness * BRIGHTNESS_SCALE)},
                    {'key': 'warmth', 'value': baf.light_warmth},
                    {'key': 'auto_mode', 'value': baf.light_auto_on}
                ])
        except Exception as e: # pylint: disable=broad-exception-caught
            self.debugLog(f"State update failed for {dev.name}: {e}")

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

        # Check if the parent bridge is configured to have a light
        bridge_id_str = dev.pluginProps.get("bridge_id")
        bridge_dev = indigo.devices[int(bridge_id_str)]
        if not bridge_dev.pluginProps.get("hasLight", True):
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
        baf_warmth_value = int(((temp_k - 2700) / (6500 - 2700)) * 1000)
        baf_warmth_value = max(0, min(1000, baf_warmth_value))  # Clamp range
        self.infoLog(f"Setting Light color temp {temp_k}K (BAF value: {baf_warmth_value}) on {dev.name}") # pylint: disable=line-too-long
        asyncio.run_coroutine_threadsafe(baf.async_set_light_warmth(baf_warmth_value), self.loop)

    def actionSetFanMode(self, action, dev): # pylint: disable=invalid-name
        """Handles custom menu actions for fan modes (Auto, Whoosh, Eco, Reverse)."""
        baf = self._get_baf_device_from_child(dev)
        if not baf:
            return

        mode = action.props.get("modeType")
        val = action.props.get("value") == "True"
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

    # --- mDNS Discovery Methods ---

    async def _start_discovery_service(self):
        """Starts the mDNS browser in the background loop."""
        self.debugLog("Starting mDNS discovery service...")
        self.azc = AsyncZeroconf()
        # TODO: Assuming the service type is '_baf_fan._tcp.local.'
        self.browser = AsyncServiceBrowser(
          self.azc.zeroconf,
          "_baf_fan._tcp.local.",
          handlers=[self]
        )

    def update_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        """Callback when a service is updated (Async method for listener)."""
        asyncio.run_coroutine_threadsafe(self._async_get_service_info(zc, type_, name), self.loop)

    def remove_service(self, zc: Zeroconf, type_: str, name: str) -> None:   # pylint: disable=unused-argument
        """Callback when a service is removed."""
        self.debugLog(f"Service {name} removed")
        if name in DISCOVERED_FANS:
            del DISCOVERED_FANS[name]

    def add_service(self, zc: Zeroconf, type_: str, name: str) -> None:
        """Callback when a new service is found."""
        asyncio.run_coroutine_threadsafe(self._async_get_service_info(zc, type_, name), self.loop)

    async def _async_get_service_info(self, zc, type_, name):
        """Retrieves service info asynchronously and updates global list."""
        info = await zc.get_service_info(type_, name)
        if info and info.addresses:
            # Convert binary IP address to string format
            address = socket.inet_ntoa(info.addresses[0])
            hostname = info.server.strip('.') if info.server else name
            DISCOVERED_FANS[name] = {"address": address, "name": hostname}
            self.debugLog(f"Discovered BAF: {hostname} at {address}")

    def getDiscoveredDevices(self, filter="", valuesDict=None, typeId="", targetId=0):  # pylint: disable=unused-argument, redefined-builtin, invalid-name
        """Called by Devices.xml to populate the dropdown list in the UI."""
        device_list = [("manual", "Manual Input (enter IP/Hostname below)")]
        for _, info in DISCOVERED_FANS.items():
            device_list.append((info['address'], info['name'] + f" ({info['address']})"))
        return device_list
    # --- End mDNS Discovery Methods ---
