#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
Indigo Plugin for Big Ass Fans (BAF) i6/Haiku devices.

This plugin bridges Indigo's synchronous Python environment with the
asynchronous aiobafi6 library using a background asyncio event loop.
It supports separate Indigo devices for fan motor and light control,
full status synchronization, and robust error handling with retries.
"""

import asyncio
import threading
import indigo
from aiobafi6 import BAFDevice

# Define constants for clarity and to avoid Pylint 'magic number' warnings
# Indigo brightness is 0-100. BAF fan speed is 0-7 (approx 14% each step)
SPEED_SCALE = 100.0 / 7.0
# BAF light brightness is 0-16 (approx 6.25% each step)
BRIGHTNESS_SCALE = 100.0 / 16.0


class Plugin(indigo.PluginBase):
    """
    Main Indigo Plugin class responsible for managing BAF devices.
    """
    def __init__(self, pluginId, pluginDisplayName, pluginVersion, pluginPrefs): # pylint: disable=invalid-name
        """Initialize plugin, data structures, and the async thread."""
        super().__init__(pluginId, pluginDisplayName, pluginVersion, pluginPrefs)
        self.active_connections = {}       # {IP: BAFDevice}
        self.hardware_to_indigo_map = {}   # {IP: [ids]}
        self.reconnect_tasks = {}          # {IP: Task}

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
        Configures IP address and starts connection supervisor if needed.
        """
        ip_address = dev.pluginProps.get("address")
        if not ip_address:
            self.errorLog(f"Device '{dev.name}' configuration error: Missing IP Address.")
            dev.setErrorStateOnServer("no ip")
            return

        self.infoLog(f"Starting communication for '{dev.name}' at {ip_address}")

        if ip_address not in self.hardware_to_indigo_map:
            self.hardware_to_indigo_map[ip_address] = []
        if dev.id not in self.hardware_to_indigo_map[ip_address]:
            self.hardware_to_indigo_map[ip_address].append(dev.id)

        # Start supervisor if not already running for this hardware
        if ip_address not in self.reconnect_tasks:
            self.debugLog(f"Launching connection supervisor task for {ip_address}")
            self.reconnect_tasks[ip_address] = asyncio.run_coroutine_threadsafe(
                self._connection_supervisor(ip_address), self.loop
            )

    async def _connection_supervisor(self, ip_address):
        """
        Manages hardware connection lifecycle with exponential backoff and logging.
        This task runs forever in the background loop.
        """
        backoff = 5.0
        max_backoff = 300.0

        while True:
            try:
                self.debugLog(f"Supervisor: Attempting aiobafi6 connection to {ip_address}")
                baf = BAFDevice(ip_address)

                def state_callback(device):
                    """Callback triggered by aiobafi6 when device state changes."""
                    self.debugLog(f"Received update from BAF at {ip_address}")
                    for dev_id in self.hardware_to_indigo_map.get(ip_address, []):
                        indigo_dev = indigo.devices.get(dev_id, None)
                        if indigo_dev:
                            self._update_indigo_states(indigo_dev, device)

                baf.add_callback(state_callback)
                self.active_connections[ip_address] = baf

                # Clear error states in Indigo on successful start/reconnect
                for dev_id in self.hardware_to_indigo_map.get(ip_address, []):
                    indigo.devices[dev_id].updateStateOnServer(
                      "onOffState",
                      indigo.devices[dev_id].onState
                    )

                # Wait for initial data and maintain connection until it drops
                await baf.async_run()

                # If async_run returns without exception, the connection was lost gracefully
                self.warnLog(f"BAF connection at {ip_address} was closed cleanly.")
                raise Exception("Connection closed") # pylint: disable=broad-exception-raised

            except Exception as e: # pylint: disable=broad-exception-caught
                # Handle connection errors with retry logic
                self.active_connections.pop(ip_address, None)
                self.errorLog(f"BAF Connection Error ({ip_address}): {str(e)}")

                # Mark Indigo devices as offline
                for dev_id in self.hardware_to_indigo_map.get(ip_address, []):
                    dev = indigo.devices.get(dev_id, None)
                    if dev:
                        dev.setErrorStateOnServer("offline")

                self.warnLog(f"Reconnecting to {ip_address} in {backoff} seconds...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

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
        ip_address = dev.pluginProps.get("address")
        baf = self.active_connections.get(ip_address)
        if not baf:
            self.warnLog(f"Cannot control Fan '{dev.name}': Device is offline.")
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
        ip_address = dev.pluginProps.get("address")
        baf = self.active_connections.get(ip_address)
        if not baf:
            self.warnLog(f"Cannot control Light '{dev.name}': Device is offline.")
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
        ip_address = dev.pluginProps.get("address")
        baf = self.active_connections.get(ip_address)
        if not baf:
            self.warnLog(f"Cannot set color temperature for '{dev.name}': Device is offline.")
            return

        # Simple linear approximation map from Kelvin (2700-6500) to BAF 0-1000 range
        temp_k = action.actionValue
        baf_warmth_value = int(((temp_k - 2700) / (6500 - 2700)) * 1000)
        baf_warmth_value = max(0, min(1000, baf_warmth_value))  # Clamp range
        self.infoLog(f"Setting Light color temp {temp_k}K (BAF value: {baf_warmth_value}) on {dev.name}") # pylint: disable=line-too-long
        asyncio.run_coroutine_threadsafe(baf.async_set_light_warmth(baf_warmth_value), self.loop)

    def actionSetFanMode(self, action, dev): # pylint: disable=invalid-name
        """Handles custom menu actions for fan modes (Auto, Whoosh, Eco, Reverse)."""
        ip_address = dev.pluginProps.get("address")
        baf = self.active_connections.get(ip_address)
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
        ip_address = dev.pluginProps.get("address")
        baf = self.active_connections.get(ip_address)
        if not baf:
            return

        if action.props.get("modeType") == "auto_mode":
            val = action.props.get("value") == "True"
            self.infoLog(f"Setting Light auto_mode to {val} on {dev.name}")
            asyncio.run_coroutine_threadsafe(baf.async_set_light_auto_on(val), self.loop)

    def deviceStopComm(self, dev): # pylint: disable=invalid-name
        """Called by Indigo when a device is disabled, handles cleanup."""
        ip_address = dev.pluginProps.get("address")
        if ip_address in self.hardware_to_indigo_map:
            self.hardware_to_indigo_map[ip_address].remove(dev.id)
            if not self.hardware_to_indigo_map[ip_address]:
                self.infoLog(f"No active devices for {ip_address}. Shutting down connection.")
                # Cancel the supervisor task and close the BAF connection
                task = self.reconnect_tasks.pop(ip_address, None)
                if task:
                    task.cancel()
                baf = self.active_connections.pop(ip_address, None)
                if baf:
                    self.loop.call_soon_threadsafe(baf.async_stop)

    def shutdown(self):
        """Called by Indigo when the plugin is globally disabled."""
        self.infoLog("Plugin shutting down. Stopping background loop.")
        self.loop.stop()
