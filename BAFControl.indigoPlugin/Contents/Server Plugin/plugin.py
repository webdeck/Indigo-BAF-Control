#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
Indigo Plugin for Big Ass Fans (BAF) i6/Haiku devices.

This plugin bridges Indigo's synchronous Python environment with the
asynchronous aiobafi6 library using a background asyncio event loop.
Parent Fan device manages child light device if applicable.
"""

import asyncio
import logging
import socket
import threading
import indigo
from aiobafi6 import Device, Service, ServiceBrowser
from zeroconf.asyncio import AsyncZeroconf


# Device Types
FAN_DEVICE_TYPE = "bafFan"
LIGHT_DEVICE_TYPE = "bafLight"

# Indigo brightness is 0-100. BAF fan speed is 0-7 (approx 14% each step)
SPEED_SCALE = 100.0 / 7.0
# BAF light brightness is 0-16 (approx 6.25% each step)
BRIGHTNESS_SCALE = 100.0 / 16.0

# Global dictionary for discovered devices: {Service_Name: {address, display}}
DISCOVERED_FANS = {}


class Plugin(indigo.PluginBase): # pylint: disable=too-many-public-methods
    """Main Plugin class managing parent-child BAF hardware."""

    def __init__(self, pluginId, pluginDisplayName, pluginVersion, pluginPrefs): # pylint: disable=invalid-name
        """Initialize plugin, data structures, and the async thread."""
        super().__init__(pluginId, pluginDisplayName, pluginVersion, pluginPrefs)
        self._set_log_level(pluginPrefs)

        self.active_connections = {}  # {FAN_ID: Device}
        self.reconnect_tasks = {}     # {FAN_ID: Task}
        self.light_to_fan_map = {}    # {LIGHT_ID: FAN_ID}
        self.azc = None
        self.browser = None

        # Initialize background loop for asynchronous I/O
        self.loop = asyncio.new_event_loop()
        self.async_thread = threading.Thread(
            target=self._run_async_loop,
            daemon=True
        )
        self.async_thread.start()
        self.logger.info("BAF Plugin initialized and background thread started.")

        # Start discovery browser
        asyncio.run_coroutine_threadsafe(
            self._start_discovery_service(),
            self.loop
        )


    def closedPrefsConfigUi(self, valuesDict, userCancelled): # pylint: disable=invalid-name
        """Called by Indigo when the plugin preferences dialog is closed"""
        if not userCancelled:
            self._set_log_level(valuesDict)


    def _run_async_loop(self):
        """Internal method to start and maintain the asyncio loop."""
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()


    def _set_log_level(self, plugin_prefs):
        """Set logging level based on plugin preferences"""
        log_level = int(plugin_prefs.get("logLevel", logging.INFO))
        self.indigo_log_handler.setLevel(log_level)
        self.plugin_file_handler.setLevel(log_level)
        logging.getLogger("aiobafi6").setLevel(log_level)
        self.logger.debug(f"Log level set to {log_level}")



    # --- Device Lifecycle ---

    def validateDeviceConfigUi(self, valuesDict, typeId, devId):  # pylint: disable=invalid-name,unused-argument
        """Called by Indigo to validate the device configuration"""
        self.logger.debug(
            f"Validating config for device {devId}: {valuesDict}"
        )
        errors = indigo.Dict()

        ip_address = self._get_ip_address_from_config(valuesDict)
        if ip_address is None:
            errors["manual_address"] = "Invalid IP Address or Hostname"

        port = self._get_port_from_config(valuesDict)
        if port is None:
            errors["manual_port"] = "Invalid Port Number"

        service = self._get_service_from_config(valuesDict)
        if service is None:
            errors["selected_device"] = "Invalid Device Selected"

        if len(errors) > 0:
            self.logger.debug(
                f"Invalid config for device {devId}: {valuesDict}, {errors}"
            )
            return (False, valuesDict, errors)

        valuesDict["address"] = self._get_service_id(service)
        self.logger.debug(
            f"Valid config for device {devId}: {valuesDict}"
        )
        return (True, valuesDict)


    def closedDeviceConfigUi(self, valuesDict, userCancelled, typeId, devId):  # pylint: disable=invalid-name,unused-argument
        """Called by Indigo to save the device configuration"""
        if not userCancelled:
            self.logger.debug(
                f"Saving config for device {devId}: {valuesDict}"
            )


    def deviceStartComm(self, dev): # pylint: disable=invalid-name
        """Called by Indigo when a device is enabled."""
        if dev.deviceTypeId == FAN_DEVICE_TYPE:
            if not dev.address:
                self.logger.error(f"Device {dev.id} has and invalid address")
                dev.setErrorStateOnServer("invalid address")
                return

            # Start connection supervisor
            if dev.id not in self.reconnect_tasks:
                self.logger.info(
                    f"Starting communication for '{dev.name}' at {dev.address}"
                )
                self.reconnect_tasks[dev.id] = asyncio.run_coroutine_threadsafe(
                    self._connection_supervisor(dev.id, dev.address),
                    self.loop
                )

        elif dev.deviceTypeId == LIGHT_DEVICE_TYPE:
            # Re-map light on startup or if enabled
            for fan in indigo.devices.withFilter(FAN_DEVICE_TYPE):
                if fan.pluginProps.get("child_light_id") == dev.id:
                    self.light_to_fan_map[dev.id] = fan.id


    def deviceStopComm(self, dev): # pylint: disable=invalid-name
        """Called by Indigo when a device is disabled."""
        if dev.deviceTypeId == FAN_DEVICE_TYPE:
            light_id = dev.pluginProps.get("child_light_id")
            if light_id:
                try:
                    indigo.device.delete(light_id)
                except: # pylint: disable=bare-except
                    pass

            self._stop_communication(dev.id)


    def _get_ip_address_from_config(self, values_dict):
        """Gets the IP address from the config, or None if invalid."""
        ip_address = None
        service_id = values_dict.get("selected_device")
        self.logger.debug(f"Reading selected_device = {service_id}")
        # Determine address from UI (Manual vs Discovery)
        if service_id == "manual":
            ip_address = values_dict.get("manual_address")
            self.logger.debug(f"Reading manual_address = {ip_address}")

            # Validate address resolution for manual entry
            if ip_address:
                try:
                    socket.gethostbyname(ip_address)
                    self.logger.debug(f"Validated IP address {ip_address}")
                except socket.gaierror:
                    self.logger.exception(f"gethostbyname({ip_address})")
        elif service_id:
            service = DISCOVERED_FANS[service_id]
            if service:
                self.logger.debug(f"Selected device: {service}")
                ip_address = service.ip_addresses[0]
            else:
                self.logger.error(f"Unable to find selected device {service_id}")

        self.logger.debug(f"IP address is {ip_address}")
        return ip_address


    def _get_port_from_config(self, values_dict):
        """Gets the port from the config, or None if invalid."""
        port = None
        service_id = values_dict.get("selected_device")
        self.logger.debug(f"Reading selected_device = {service_id}")
        # Determine port from UI (Manual vs Discovery)
        if service_id == "manual":
            portstr = values_dict.get("manual_port")
            self.logger.debug(f"Reading manual_port = {portstr}")
            try:
                port = int(portstr)
            except ValueError:
                self.logger.exception(f"Invalid port specified: {portstr}")
        elif service_id:
            service = DISCOVERED_FANS[service_id]
            if service:
                self.logger.debug(f"Selected device: {service}")
                port = service.port
            else:
                self.logger.error(f"Unable to find selected device {service_id}")

        self.logger.debug(f"Port is {port}")
        if port < 1 or port > 65535:
            self.logger.error(f"Invalid port specified: {port}")
            port = None

        return port


    def _get_service_from_config(self, values_dict):
        """Gets the Servie object from the config, or None if invalid."""
        service = None
        service_id = values_dict.get("selected_device")
        self.logger.debug(f"Reading selected_device = {service_id}")
        # Determine Service from UI (Manual vs Discovery)
        if service_id == "manual":
            ip_address = self._get_ip_address_from_config(values_dict)
            port = self._get_port_from_config(values_dict)
            if ip_address and port:
                service = Service([ip_address], port)
                service_id = self._get_service_id(service)
                DISCOVERED_FANS[service_id] = service
            else:
                service_id = None

        if service_id:
            service = DISCOVERED_FANS[service_id]
        else:
            self.logger.error(f"Unable to find selected service from {values_dict}")

        return service


    def _get_baf_instance(self, dev):
        """Helper to find the active connection for either a fan or light device."""
        fan_id = dev.id
        if dev.deviceTypeId == LIGHT_DEVICE_TYPE and dev.id is not None:
            fan_id = self.light_to_fan_map.get(dev.id)
        if fan_id is not None:
            return self.active_connections.get(fan_id)
        return None


    async def _connection_supervisor(self, fan_id, service):
        """
        Manages hardware connection and child light lifecycle.
        This task runs forever in the background loop.
        """
        backoff = 5.0
        max_backoff = 300.0

        while True:
            try:
                service_id = self._get_service_id(service)
                self.logger.debug(
                    f"Attempting connection for Fan ID {fan_id} to {service_id}"
                )
                baf = Device(service)

                def state_callback(device):
                    # Update Fan visibility state based on hardware discovery
                    fan_dev = indigo.devices.get(fan_id)
                    if not fan_dev:
                        return

                    # --- Automatic Child Light Creation ---
                    light_id = fan_dev.pluginProps.get("child_light_id")
                    if device.has_light and not light_id:
                        new_light = indigo.device.create(
                            protocol=indigo.kProtocol.Plugin,
                            address=service_id,
                            name=f"{fan_dev.name} Light",
                            deviceTypeId=LIGHT_DEVICE_TYPE,
                            folder=fan_dev.folderId
                        )
                        props = fan_dev.pluginProps
                        props["child_light_id"] = new_light.id
                        fan_dev.replacePluginPropsOnServer(props)
                        self.light_to_fan_map[new_light.id] = fan_id
                        self.logger.info(
                            f"Automatically created child light for {fan_dev.name}"
                        )

                    # --- Update Indigo States ---
                    self._update_states(fan_dev, device)
                    if device.has_light and light_id:
                        light_dev = indigo.devices.get(light_id)
                        if light_dev:
                            self._update_states(light_dev, device)

                baf.add_callback(state_callback)
                self.active_connections[fan_id] = baf

                # Clear error states in Indigo on successful start/reconnect
                indigo.devices[fan_id].setErrorStateOnServer(None)

                # Wait for initial data and maintain connection until it drops
                await baf.async_run()

                # If we get here, the conneciton was closed
                raise Exception("Connection closed") # pylint: disable=broad-exception-raised

            except Exception: # pylint: disable=broad-exception-caught
                self.active_connections.pop(fan_id, None)
                self.logger.exception(f"Connection lost to {service_id}")
                indigo.devices[fan_id].setErrorStateOnServer("offline")

                self.logger.warn(
                    f"Reconnecting to Fan ID {fan_id} in {backoff} seconds..."
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)


    def _update_states(self, dev, baf):
        """Maps BAF properties to native and custom Indigo device states."""
        try:
            if dev.deviceTypeId == FAN_DEVICE_TYPE:
                dev.updateStatesOnServer([
                    {'key': 'speed', 'value': baf.speed},
                    {'key': 'onOffState', 'value': baf.fan_on},
                    {'key': 'auto_mode', 'value': baf.fan_auto_on},
                    {'key': 'whoosh_mode', 'value': baf.whoosh_mode_on},
                    {'key': 'eco_mode', 'value': baf.eco_mode_on},
                    {'key': 'reverse_direction', 'value': baf.reverse_direction_on}
                ])
            elif dev.deviceTypeId == LIGHT_DEVICE_TYPE:
                dev.updateStatesOnServer([
                    {'key': 'onOffState', 'value': baf.light_on},
                    {'key': 'brightness',
                     'value': (baf.light_brightness * BRIGHTNESS_SCALE)},
                    {'key': 'auto_mode', 'value': baf.light_auto_on},
                    {'key': 'warmth', 'value': baf.light_warmth}
                ])
        except Exception: # pylint: disable=broad-exception-caught
            self.logger.exception(f"State update failed for {dev.name}")


    def _dispatch_baf_command(self, dev, baf_method, *args):
        """
        Retrieves the correct BAF instance and dispatches an 
        asynchronous command to the background loop.
        """
        baf = self._get_baf_instance(dev)
        if not baf:
            self.logger.error(
                f"Command {baf_method} failed: '{dev.name}' is offline or not linked."
            )
            return

        # Dynamically get the method from the BAF instance and schedule it
        method = getattr(baf, baf_method, None)
        if method:
            self.logger.debug(f"Dispatching {baf_method} for '{dev.name}'")
            asyncio.run_coroutine_threadsafe(method(*args), self.loop)
        else:
            self.logger.error(
                f"Invalid hardware method: {baf_method} for '{dev.name}'"
            )


    def _stop_communication(self, fan_id):
        """Centralized cleanup for stopping a specific hardware connection."""
        task = self.reconnect_tasks.pop(fan_id, None)
        if task:
            self.logger.debug(
                f"Canceling reconnection supervisor for Fan ID {fan_id}"
            )
            task.cancel()

        baf = self.active_connections.pop(fan_id, None)
        if baf:
            self.logger.debug(
                f"Stopping connection for Fan ID {fan_id}"
            )
            self.loop.call_soon_threadsafe(baf.async_stop)

        # Remove all entries in the light map associated with this fan
        self.light_to_fan_map = {k: v for k, v in self.light_to_fan_map.items() if v != fan_id}



    # --- Device Discovery ---

    async def _start_discovery_service(self):
        """Starts device discovery with this pluugin as the callback handler."""
        if not self.azc:
            self.logger.debug("Starting BAF device discovery")
            self.azc = AsyncZeroconf()
            self.browser = ServiceBrowser(self.azc.zeroconf, self)


    async def _stop_discovery_service(self):
        """Starts device discovery with this pluugin as the callback handler."""
        if self.azc:
            self.logger.debug("Stopping BAF device discovery")
            self.azc.async_close()
            self.azc = None
            self.browser = None


    def _get_service_id(self, service):
        service_id = None
        if service and service.ip_addresses and len(service.ip_addresses) > 0:
            service_id = f"{service.ip_addresses[0]}:{service.port}"
        return service_id


    def add_service(self, service):
        """Callback from aiobafi6.discovery when a verified fan is found."""
        self.logger.info(
            f"Discovered BAF Device: {service.device_name} {service.ip_addresses} {service.port}"
        )
        service_id = self._get_service_id(service)
        if service_id:
            self.logger.debug(f"Saving BAF Device with id {service_id}")
            DISCOVERED_FANS[service_id] = service


    def remove_service(self, service):
        """Callback from aiobafi6.discovery when a fan is removed."""
        self.logger.info(
            f"Removed BAF Device: {service.device_name} {service.ip_addresses} {service.port}"
        )
        service_id = self._get_service_id(service)
        if service_id:
            self.logger.debug(f"Removing BAF Device with id {service_id}")
            DISCOVERED_FANS.pop(service_id, None)


    def getDiscoveredDevices(self, filter="", valuesDict=None, typeId="", targetId=0): # pylint: disable=invalid-name,unused-argument,redefined-builtin
        """Populates the ConfigUI with verified BAF devices."""
        items = [("manual", "Manual Input (Enter IP/Hostname below)")]
        for service_id, service in DISCOVERED_FANS.items():
            name = "Unnamed Fan"
            if service.device_name:
                name = service.device_name
            display_name = f"{name} [{service.model}] ({service_id})"
            items.append((service_id, display_name))
        self.logger.debug(f"Discovered BAF devices: {items}")
        return items



    # --- Standard Indigo Action Callbacks ---

    def actionControlFan(self, action, dev): # pylint: disable=invalid-name
        """Handles standard Indigo Fan actions (On/Off/Speed)."""
        if action.deviceAction == indigo.kDeviceAction.TurnOn:
            self._dispatch_baf_command(dev, "async_set_fan_on", True)
        elif action.deviceAction == indigo.kDeviceAction.TurnOff:
            self._dispatch_baf_command(dev, "async_set_fan_on", False)
        elif action.deviceAction == indigo.kDeviceAction.SetBrightness:
            speed = int(action.actionValue / SPEED_SCALE)
            self._dispatch_baf_command(dev, "async_set_speed", speed)


    def actionControlDimmerRelay(self, action, dev): # pylint: disable=invalid-name
        """Handles standard Indigo Light actions (On/Off/Brightness)."""
        if action.deviceAction == indigo.kDeviceAction.TurnOn:
            self._dispatch_baf_command(dev, "async_set_light_on", True)
        elif action.deviceAction == indigo.kDeviceAction.TurnOff:
            self._dispatch_baf_command(dev, "async_set_light_on", False)
        elif action.deviceAction == indigo.kDeviceAction.SetBrightness:
            brightness = int(action.actionValue / BRIGHTNESS_SCALE)
            self._dispatch_baf_command(dev, "async_set_light_brightness", brightness)



    # --- Fan Action Callbacks ---

    def actionEnableFanAuto(self, action, dev): # pylint: disable=invalid-name,unused-argument
        """Handles enabling fan auto mode"""
        self._dispatch_baf_command(dev, "async_set_fan_auto_on", True)


    def actionDisableFanAuto(self, action, dev): # pylint: disable=invalid-name,unused-argument
        """Handles disabling fan auto mode"""
        self._dispatch_baf_command(dev, "async_set_fan_auto_on", False)


    def actionEnableWhoosh(self, action, dev): # pylint: disable=invalid-name,unused-argument
        """Handles enabling fan whoosh mode"""
        self._dispatch_baf_command(dev, "async_set_whoosh_mode_on", True)


    def actionDisableWhoosh(self, action, dev): # pylint: disable=invalid-name,unused-argument
        """Handles disabling fan whoosh mode"""
        self._dispatch_baf_command(dev, "async_set_whoosh_mode_on", False)


    def actionEnableEco(self, action, dev): # pylint: disable=invalid-name,unused-argument
        """Handles enabling fan eco mode"""
        self._dispatch_baf_command(dev, "async_set_eco_mode_on", True)


    def actionDisableEco(self, action, dev): # pylint: disable=invalid-name,unused-argument
        """Handles disabling fan eco mode"""
        self._dispatch_baf_command(dev, "async_set_eco_mode_on", False)

    def actionEnableReverse(self, action, dev): # pylint: disable=invalid-name,unused-argument
        """Handles enabling fan reverse direction"""
        self._dispatch_baf_command(dev, "async_set_reverse_direction_on", True)


    def actionDisableReverse(self, action, dev): # pylint: disable=invalid-name,unused-argument
        """Handles disabling fan reverse direction"""
        self._dispatch_baf_command(dev, "async_set_reverse_direction_on", False)



    # --- Light Action Callbacks ---

    def actionEnableLightAuto(self, action, dev): # pylint: disable=invalid-name,unused-argument
        """Handles enabling light auto mode"""
        self._dispatch_baf_command(dev, "async_set_light_auto_on", True)


    def actionDisableLightAuto(self, action, dev): # pylint: disable=invalid-name,unused-argument
        """Handles disabling light auto mode"""
        self._dispatch_baf_command(dev, "async_set_light_auto_on", False)


    def actionControlColorTemperature(self, action, dev): # pylint: disable=invalid-name,unused-argument
        """Handles setting light color temperature"""
        temp_k = action.actionValue
        warmth = int(((temp_k - 2700) / (6500 - 2700)) * 1000)
        warmth = max(0, min(1000, warmth))
        self._dispatch_baf_command(dev, "async_light_warmth", warmth)



    # --- Shutdown and Cleanup ---

    def shutdown(self):
        """Called by Indigo when the plugin is globally disabled."""
        self.logger.info("Plugin shutting down. Stopping background loop.")

        # Stop all active fan connections and supervisors
        for fan_id in list(self.active_connections.keys()):
            self._stop_communication(fan_id)

        # Stop discovery service
        asyncio.run_coroutine_threadsafe(
            self._stop_discovery_service(),
            self.loop
        )

        # Stop background loop
        self.loop.stop()
