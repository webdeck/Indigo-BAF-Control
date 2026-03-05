#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
Indigo Plugin for Big Ass Fans (BAF) i6/Haiku devices.

This plugin bridges Indigo's synchronous Python environment with the
asynchronous aiobafi6 library using a background asyncio event loop.
Parent Fan device manages child light device if applicable.
"""

from __future__ import annotations
import asyncio
import concurrent.futures
import logging
from queue import Queue, Empty
import socket
import threading
from typing import Any, Callable, Optional
# noinspection PyUnresolvedReferences
import indigo  # pylint: disable=import-error
from aiobafi6 import (Device as BAFDevice, Service as BAFService, OffOnAuto)
from device_discovery import BAFDeviceDiscoveryManager, ServiceId

# Typing
DeviceId = int
DeviceMenuItem = tuple[ServiceId, str]

# Device Types
FAN_DEVICE_TYPE = "bafFan"
LIGHT_DEVICE_TYPE = "bafLight"

# Indigo fan speed index is 0-3; BAF fan speed is 0-7
INDIGO_SPEED_MAX_INDEX = 3.0
BAF_SPEED_MAX = 7.0
INDIGO_TO_BAF_SPEED_INDEX_RATIO = INDIGO_SPEED_MAX_INDEX / BAF_SPEED_MAX

# Indigo light brightness is 0-100; BAF light brightness is 0-16
INDIGO_BRIGHTNESS_MAX = 100.0
BAF_BRIGHTNESS_MAX = 16.0
INDIGO_TO_BAF_BRIGHTNESS_RATIO = INDIGO_BRIGHTNESS_MAX / BAF_BRIGHTNESS_MAX

# Service ID for manual IP/Port entry
SERVICE_ID_MANUAL = "manual"
# Default BAF device port
DEFAULT_PORT = 31415


class Plugin(indigo.PluginBase):  # pylint: disable=too-many-public-methods,too-many-instance-attributes
    """Main Plugin class managing communication with BAF/Haiku devices."""


    # --- Plugin Lifecycle ---

    def __init__(self, pluginId, pluginDisplayName, pluginVersion, pluginPrefs):  # pylint: disable=invalid-name
        """Initialize plugin and data structures."""
        super().__init__(pluginId, pluginDisplayName, pluginVersion, pluginPrefs)
        self._set_log_level(pluginPrefs)

        self._lock = threading.Lock()
        self.fan_to_baf_map: dict[DeviceId, BAFDevice] = {}
        self.baf_to_fan_map: dict[BAFDevice, DeviceId] = {}
        self.baf_connections: dict[DeviceId, asyncio.Future] = {}
        self.light_to_fan_map: dict[DeviceId, DeviceId] = {}  # {LIGHT_ID: FAN_ID}
        self.fan_availability: dict[DeviceId, bool] = {}

        self.discovery_manager = BAFDeviceDiscoveryManager(self.logger)
        self.event_loop: Optional[asyncio.AbstractEventLoop] = None
        self.event_loop_thread: Optional[threading.Thread] = None
        self._device_operations_queue: Optional[Queue[Callable]] = None
        self._stop_device_ops_event = threading.Event()


    def startup(self):
        """Called by Indigo wwhen the plugin is enabled."""
        self.logger.info("Starting BAFControl Plugin...")

        # Initialize background loop for asynchronous I/O
        self.event_loop = asyncio.new_event_loop()
        self.event_loop_thread = threading.Thread(
            target=self._run_event_loop,
            daemon=True
        )
        self.event_loop_thread.start()

        # Start discovery browser
        asyncio.run_coroutine_threadsafe(
            self.discovery_manager.start(),
            self.event_loop
        )

        self.logger.info("BAFControl Plugin started.")


    def closedPrefsConfigUi(self, valuesDict, userCancelled):  # pylint: disable=invalid-name
        """Called by Indigo when the plugin preferences dialog is closed"""
        if not userCancelled:
            self._set_log_level(valuesDict)


    def runConcurrentThread(self):  # pylint: disable=invalid-name
        """Called by Indigo when the plugin is enabled."""
        self.logger.info("Started device operations processor thread.")
        self._stop_device_ops_event.clear()
        self._device_operations_queue = Queue()

        try:
            while not self._stop_device_ops_event.is_set():
                try:
                    op: Callable = self._device_operations_queue.get(timeout=1.0)
                    op()
                except Empty:
                    pass
        except Exception as ex:   # pylint: disable=broad-exception-caught
            self.logger.exception(f"Exception in device operations processor thread: {ex}")
        finally:
            self._device_operations_queue = None
            self.logger.info("Stopped device operations processor thread.")


    def stopConcurrentThread(self):  # pylint: disable=invalid-name
        """Called by Indigo when the plugin is disabled."""
        self.logger.info("Stopping device operations processor thread...")
        self._stop_device_ops_event.set()


    def shutdown(self):
        """Called by Indigo when the plugin is disabled."""
        self.logger.info("Shutting down BAFControl Plugin...")

        # Stop discovery manager
        try:
            if self.event_loop:
                asyncio.run_coroutine_threadsafe(
                    self.discovery_manager.stop(),
                    self.event_loop
                ).result(timeout=2.0)
        except (asyncio.TimeoutError, concurrent.futures.TimeoutError):
            self.logger.warning("Timed out waiting for discovery to stop.")
        finally:
            # Stop all active fan connections and supervisors
            with self._lock:
                fan_ids = list(self.fan_to_baf_map.keys())
            for fan_id in fan_ids:
                self._stop_baf_connection(fan_id)

            self._stop_event_loop()

            with self._lock:
                self.fan_to_baf_map.clear()
                self.baf_to_fan_map.clear()
                self.baf_connections.clear()
                self.light_to_fan_map.clear()
                self.fan_availability.clear()

            self.logger.info("Plugin shutdown complete.")


    def _add_device_operation(self, op: Callable) -> None:
        """Adds a device operation onto the queue for Indigo's concurrent thread."""
        q = self._device_operations_queue
        if q:
            q.put(op)


    def _run_event_loop(self) -> None:
        """Internal method to start and maintain the asyncio loop."""
        asyncio.set_event_loop(self.event_loop)
        try:
            self.event_loop.run_forever()
        finally:
            self.event_loop.close()
            self.logger.debug("Async loop thread terminating")


    def _stop_event_loop(self) -> None:
        """Internal method to stop and clea up the asyncio loop."""
        try:
            # Schedule shutdown in the loop
            if self.event_loop and not self.event_loop.is_closed():
                asyncio.run_coroutine_threadsafe(
                    self.event_loop.shutdown_asyncgens(),
                    self.event_loop
                ).result(timeout=2.0)
        except (asyncio.TimeoutError, concurrent.futures.TimeoutError):
            self.logger.warning("Timed out waiting for async generators to shut down.")
        except Exception as ex:  # pylint: disable=broad-exception-caught
            self.logger.debug(f"Shutdown event loop failed: {ex}")

        try:
            if self.event_loop and not self.event_loop.is_closed():
                # noinspection PyArgumentList,PyTypeChecker
                self.event_loop.call_soon_threadsafe(self.event_loop.stop)
        except RuntimeError:
            pass  # Loop already stopped

        # Wait for async thread to terminate gracefully (with timeout)
        if self.event_loop_thread and self.event_loop_thread.is_alive():
            self.event_loop_thread.join(timeout=2.0)
            if self.event_loop_thread.is_alive():
                self.logger.warning("Event loop thread did not terminate gracefully")

        self.event_loop_thread = None
        self.event_loop = None


    def _set_log_level(self, plugin_prefs: dict) -> None:
        """Set logging level based on plugin preferences"""
        log_level: int = int(plugin_prefs.get("logLevel", logging.INFO))
        self.indigo_log_handler.setLevel(log_level)
        self.plugin_file_handler.setLevel(log_level)
        logging.getLogger("aiobafi6").setLevel(log_level)
        self.logger.debug(f"Log level set to {log_level}")



    # --- Device Configuration Callbacks ---

    # noinspection PyShadowingBuiltins,PyUnusedLocal
    def getDiscoveredDevices(self, filter="", valuesDict=None, typeId="", targetId=0):  # pylint: disable=invalid-name,unused-argument,redefined-builtin
        """Populates the ConfigUI with verified BAF devices."""
        items: list[DeviceMenuItem] = []
        for service_id, service in self.discovery_manager.discovered_services.items():
            if service is not None:  # Skip deleted entries
                name = "Unnamed Fan"
                if hasattr(service, 'device_name') and service.device_name:
                    name = service.device_name
                display_name = f"{name} [{getattr(service, 'model', 'Unknown')}] ({service_id})"
                items.append((service_id, display_name))
        self.logger.debug(f"Discovered BAF devices: {items}")
        items.append((SERVICE_ID_MANUAL, "Manual Input (Enter IP/Hostname below)"))
        return items


    # noinspection PyUnusedLocal
    def validateDeviceConfigUi(self, valuesDict, typeId, devId):  # pylint: disable=invalid-name,unused-argument
        """Called by Indigo to validate the device configuration"""
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
            # noinspection PyRedundantParentheses
            return (False, valuesDict, errors)

        valuesDict["address"] = self.discovery_manager.get_service_id(service)
        self.logger.debug(
            f"Valid config for device {devId}: {valuesDict}"
        )
        # noinspection PyRedundantParentheses
        return (True, valuesDict)


    # Device Lifecycle

    def deviceStartComm(self, dev):  # pylint: disable=invalid-name
        """Called by Indigo when a device is enabled."""
        if dev.deviceTypeId != FAN_DEVICE_TYPE:
            return

        service = self._get_service_from_config(dev.pluginProps)
        if service is None:
            self.logger.error(f"Failed to get device address for device {dev.id}")
            dev.setErrorStateOnServer("invalid configuration")
            return

        # Update fan properties
        props = dict(dev.pluginProps)
        props["supportsAllOff"] = True
        props["supportsStatusRequest"] = False
        dev.replacePluginPropsOnServer(props)

        # First, cancel any existing supervisor for this fan to be safe
        self._stop_baf_connection(dev.id)

        # Start connection supervisor
        self.logger.info(
            f"Starting communication with '{dev.name}' at {dev.address}"
        )
        try:
            with self._lock:
                self.baf_connections[dev.id] = asyncio.run_coroutine_threadsafe(
                    self._start_baf_connection(dev.id, service),
                    self.event_loop
                )
        except Exception as ex:  # pylint: disable=broad-exception-caught
            self.logger.exception(f"Failed to start communication with {dev.id}: {ex}")
            dev.setErrorStateOnServer("connection failed")


    def deviceStopComm(self, dev):  # pylint: disable=invalid-name
        """Called by Indigo when a device is disabled."""
        if dev.deviceTypeId == FAN_DEVICE_TYPE:
            # First stop the connection supervisor and cleanup
            self._stop_baf_connection(dev.id)

            # Delete light sub-device as well
            light_id = dev.pluginProps.get("child_light_id")
            if light_id and light_id in indigo.devices:
                self.logger.debug(f"Queueing light deletion for {dev.name}")
                self._add_device_operation(
                    lambda: self._delete_light(light_id, dev.id)
                )

        elif dev.deviceTypeId == LIGHT_DEVICE_TYPE:
            # Remove from light to fan map
            with self._lock:
                if dev.id in self.light_to_fan_map:
                    del self.light_to_fan_map[dev.id]


    def didDeviceCommPropertyChange(self, origDev, newDev):  # pylint: disable=invalid-name
        """Called by Indigo when a device property changes to see if comm needs to restart."""
        return newDev.deviceTypeId == FAN_DEVICE_TYPE and origDev.address != newDev.address


    # Parse device configuration

    def _get_ip_address_from_config(self, values_dict: dict) -> Optional[str]:
        """Gets the IP address from the config, or None if invalid."""
        ip_address: Optional[str] = None
        service_id: ServiceId = values_dict.get("selected_device")
        if service_id == SERVICE_ID_MANUAL:
            ip_address = values_dict.get("manual_address")
            if ip_address:
                try:
                    socket.gethostbyname(ip_address)
                except socket.gaierror as ex:
                    self.logger.exception(f"Unable to validate IP address {ip_address}: {ex}")
                    ip_address = None

        elif service_id:
            service = self.discovery_manager.get_service_by_id(service_id)
            if service and hasattr(service, 'ip_addresses') and service.ip_addresses:
                ip_address = service.ip_addresses[0]
            else:
                self.logger.error(f"Unable to find selected device {service_id}")

        return ip_address


    def _get_port_from_config(self, values_dict: dict) -> Optional[int]:
        """Gets the port from the config, or None if invalid."""
        port: Optional[int] = None
        service_id: ServiceId = values_dict.get("selected_device")
        if service_id == SERVICE_ID_MANUAL:
            portstr = values_dict.get("manual_port")
            try:
                port = int(portstr)
            except (ValueError, TypeError):
                self.logger.error(
                    f"Invalid port specified: {portstr}, using default {DEFAULT_PORT} instead"
                )
        elif service_id:
            service = self.discovery_manager.get_service_by_id(service_id)
            if service:
                port = service.port
            else:
                self.logger.error(f"Unable to find selected device {service_id}")

        if port is not None and (port < 1 or port > 65535):
            self.logger.error(
                f"Invalid port specified: {port}, using default {DEFAULT_PORT} instead"
            )
            port = None

        if port is None:
            port = DEFAULT_PORT

        return port


    def _get_service_from_config(self, values_dict: dict) -> Optional[BAFService]:
        """Gets the Service object from the config, or None if invalid."""
        service: Optional[BAFService] = None
        service_id: ServiceId = values_dict.get("selected_device")
        if service_id == SERVICE_ID_MANUAL:
            ip_address  = self._get_ip_address_from_config(values_dict)
            port = self._get_port_from_config(values_dict)
            if ip_address and port:
                service = BAFService([ip_address], port)
                service_id = self.discovery_manager.get_service_id(service)
                if service_id:
                    self.discovery_manager.add_service_by_id(service, service_id)
            else:
                self.logger.error("Manual address and/or port configuration is invalid")
        else:
            service = self.discovery_manager.get_service_by_id(service_id)
            if not service:
                self.logger.error(f"Unable to find selected device {service_id}")

        return service


    # Device connection management

    async def _start_baf_connection(self, fan_id: DeviceId, service: BAFService) -> None:
        """
        Manages hardware connection and child light lifecycle.
        This method runs forever as a background Task.
        """
        service_id = self.discovery_manager.get_service_id(service)
        if not service_id:
            self.logger.error(f"Failed to get service ID for fan {fan_id}")
            return

        self.logger.debug(
            f"Connection supervisor started for Fan ID {fan_id} to {service_id}"
        )
        backoff = 5.0
        max_backoff = 300.0

        try:
            while True:
                with self._lock:
                    if fan_id not in self.baf_connections:
                        self.logger.debug(f"Connection supervisor for {fan_id} cancelled.")
                        break
                # noinspection PyBroadException
                try:
                    await self._manage_baf_connection(fan_id, service_id, service)
                    backoff = 5.0  # Reset backoff on successful connection
                except asyncio.CancelledError:  # pylint:disable=try-except-raise
                    # Propagate cancellation
                    raise
                except Exception as ex:  # pylint: disable=broad-exception-caught
                    self.logger.exception(
                        f"Reconnecting to Fan ID {fan_id} in {backoff} seconds: {ex}"
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)

        except asyncio.CancelledError:
            self.logger.debug(
                f"Connection supervisor for Fan ID {fan_id} was cancelled"
            )
        finally:
            self.logger.debug(
                f"Connection supervisor for Fan ID {fan_id} stopped"
            )


    def _stop_baf_connection(self, fan_id: DeviceId) -> None:
        """Centralized cleanup for stopping a specific hardware connection."""
        with self._lock:
            task = self.baf_connections.get(fan_id)
        if task:
            self.logger.debug(f"Canceling connection for Fan ID {fan_id}")
            try:
                task.cancel()
            except Exception as ex:  # pylint: disable=broad-exception-caught
                self.logger.exception(f"Failed to cancel task for Fan ID {fan_id}: {ex}")

        with self._lock:
            self.fan_availability.pop(fan_id, None)
            self.baf_connections.pop(fan_id, None)
            baf = self.fan_to_baf_map.pop(fan_id, None)
            if baf:
                self.baf_to_fan_map.pop(baf, None)

        self._add_device_operation(lambda: self._update_error_state(fan_id, "offline"))


    async def _manage_baf_connection(self, fan_id: DeviceId,
                                     service_id: ServiceId,
                                     service: BAFService) -> None:
        """
        Connects to a fan device and processes state callbacks.
        Keeps running for as long as the fan stays connected.
        """
        self.logger.debug(
            f"Attempting connection to Fan ID {fan_id} at {service_id}"
        )

        baf = BAFDevice(service)
        with self._lock:
            self.fan_to_baf_map[fan_id] = baf
            self.baf_to_fan_map[baf] = fan_id

        baf.add_callback(self._baf_state_callback)

        self.logger.info(
            f"Connection established to Fan ID {fan_id} at {service_id}, monitoring connection..."
        )

        try:
            await baf.async_run()
        finally:
            baf.remove_callback(self._baf_state_callback)
            self.logger.warning(f"Connection closed for Fan ID {fan_id} at {service_id}")
            self._add_device_operation(lambda: self._update_error_state(fan_id, "offline"))


    def _baf_state_callback(self, baf_device: BAFDevice) -> None:
        """Handles callback from all BAF devices"""
        with self._lock:
            fan_id: Optional[DeviceId] = self.baf_to_fan_map.get(baf_device)
        if fan_id:
            self._handle_baf_state_callback(baf_device, fan_id)
        else:
            self.logger.debug("Unable to find fan_id for BAF device callback")


    def _handle_baf_state_callback(self, baf_device: BAFDevice, fan_id: DeviceId) -> None:
        """Handles callback for a specific BAF device with state updates"""
        try:
            fan_dev: Optional[indigo.Device] = indigo.devices.get(fan_id)
            if not fan_dev:
                self.logger.debug(f"Fan device {fan_id} no longer exists, stopping callback")
                return

            self._update_fan_states(fan_dev, baf_device)

            # Handle child light device - create/delete if necessary
            light_id: Optional[DeviceId] = fan_dev.pluginProps.get("child_light_id")
            # noinspection PyUnresolvedReferences
            if baf_device.has_any_light:
                if light_id:
                    light_dev: Optional[indigo.Device] = indigo.devices.get(light_id)
                    if light_dev:
                        self._update_light_states(light_dev, baf_device)
                else:
                    self.logger.debug(f"Queueing light creation for fan {fan_id}")
                    self._add_device_operation(
                        lambda: self._create_light(fan_id, fan_dev.name,
                                                   fan_dev.address, baf_device)
                    )
            elif light_id:
                self.logger.debug(f"Queueing light deletion for fan {fan_id}")
                self._add_device_operation(
                    lambda: self._delete_light(light_id, fan_id)
                )

        except Exception as ex:  # pylint: disable=broad-exception-caught
            self.logger.exception(f"State callback error for fan {fan_id}: {ex}")


    def _create_light(self, fan_id: DeviceId, fan_name: str,
                      service_id: ServiceId, baf_device: BAFDevice) -> None:
        """Helper method to create a child light device (called on concurrent thread)."""
        fan_dev: Optional[indigo.Device] = indigo.devices.get(fan_id)
        if fan_dev and not fan_dev.pluginProps.get("child_light_id"):
            new_light: indigo.Device = indigo.device.create(
                protocol=indigo.kProtocol.Plugin,
                address=service_id,
                name=f"{fan_name} Light",
                deviceTypeId=LIGHT_DEVICE_TYPE,
                folder=fan_dev.folderId
            )

            # Update light properties
            props = dict(new_light.pluginProps)
            props["supportsAllLightsOnOff"] = True
            props["supportsAllOff"] = True
            props["SupportsColor"] = False
            props["supportsRGB"] = False
            props["supportsRGBandWhiteSimultaneously"] = False
            props["supportsStatusRequest"] = False
            props["supportsTwoWhiteLevels"] = False
            props["supportsTwoWhiteLevelsSimultaneously"] = False
            props["supportsWhite"] = False
            props["supportsWhiteTemperature"] = True
            new_light.replacePluginPropsOnServer(props)

            # Update fan properties
            props = dict(fan_dev.pluginProps)
            props["child_light_id"] = new_light.id
            fan_dev.replacePluginPropsOnServer(props)

            with self._lock:
                self.light_to_fan_map[new_light.id] = fan_id
            self.logger.info(f"Created child light device {new_light.id} for {fan_name}")

            self._update_light_states(new_light, baf_device)


    def _delete_light(self, light_id: DeviceId, fan_id: DeviceId) -> None:
        """Helper method to delete a child light device (called on concurrent thread)."""
        light_dev: Optional[indigo.Device] = indigo.devices.get(light_id)
        if light_dev:
            indigo.device.delete(light_id)
            self.logger.info(f"Removed child light device {light_id} for fan {fan_id}")

        with self._lock:
            self.light_to_fan_map.pop(light_id, None)

        fan_dev: Optional[indigo.Device] = indigo.devices.get(fan_id)
        if fan_dev and fan_dev.pluginProps.get("child_light_id"):
            props = dict(fan_dev.pluginProps)
            props.pop("child_light_id", None)
            fan_dev.replacePluginPropsOnServer(props)

    # noinspection PyUnresolvedReferences
    def _update_fan_states(self, fan_dev: indigo.Device, baf_dev: BAFDevice) -> None:
        """Maps BAF properties to native and custom Indigo device states."""
        self._update_device_available(fan_dev, baf_dev.available)
        if baf_dev.available:
            speed_index = int(min(baf_dev.speed * INDIGO_TO_BAF_SPEED_INDEX_RATIO,
                                  INDIGO_SPEED_MAX_INDEX))
            on_off_state = baf_dev.speed > 0
            auto_mode = baf_dev.fan_mode == OffOnAuto.AUTO
            states = [
                {'key': 'speedIndex', 'value': speed_index},
                {'key': 'speedLevel', 'value': baf_dev.speed_percent},
                {'key': 'onOffState', 'value': on_off_state},
                {'key': 'auto_mode', 'value': auto_mode},
                {'key': 'whoosh_mode', 'value': baf_dev.whoosh_enable},
                {'key': 'eco_mode', 'value': baf_dev.eco_enable},
                {'key': 'reverse_direction', 'value': baf_dev.reverse_enable}
            ]
            self._add_device_operation(
                lambda: self._update_device_states_on_server(fan_dev, states)
            )


    def _update_device_states_on_server(self, dev: indigo.Device, states: list[dict]) -> None:
        """Updates the device states on the server (called on concurrent thread)."""
        try:
            dev.updateStatesOnServer(states)
        except (KeyError, AttributeError, TypeError) as ex:
            self.logger.exception(f"State update failed for {dev.name}: {ex}")


    # noinspection PyUnresolvedReferences
    def _update_light_states(self, light_dev: indigo.Device, baf_dev: BAFDevice) -> None:
        """Maps BAF properties to native and custom Indigo device states."""
        self._update_device_available(light_dev, baf_dev.available)
        if baf_dev.available:
            brightness = int(min(baf_dev.light_brightness_level * INDIGO_TO_BAF_BRIGHTNESS_RATIO,
                                 INDIGO_BRIGHTNESS_MAX))
            on_off_state = brightness > 0
            auto_mode = baf_dev.light_mode == OffOnAuto.AUTO
            states = [
                {'key': 'onOffState', 'value': on_off_state},
                {'key': 'brightnessLevel', 'value': brightness},
                {'key': 'auto_mode', 'value': auto_mode},
                {'key': 'whiteTemperature', 'value': baf_dev.light_color_temperature}
            ]
            self._add_device_operation(
                lambda: self._update_device_states_on_server(light_dev, states)
            )


    def _update_device_available(self, dev: indigo.Device, available: bool) -> None:
        """Updates device availability state on the server (uses concurrent thread.)"""
        with self._lock:
            is_available = self.fan_availability.get(dev.id, False)
        if available != is_available:
            with self._lock:
                self.fan_availability[dev.id] = available
            self._add_device_operation(
                lambda: dev.setErrorStateOnServer("offline" if not available else None)
            )


    def _update_error_state(self, fan_id: DeviceId, state: Optional[str]) -> None:
        """Updates the error state for the fan device (and light sub-device if applicable)"""
        with self._lock:
            self.fan_availability[fan_id] = state is None
        fan = indigo.devices.get(fan_id)
        if fan:
            fan.setErrorStateOnServer(state)
            light_id = fan.pluginProps.get("child_light_id")
            if light_id:
                light = indigo.devices.get(light_id)
                if light:
                    light.setErrorStateOnServer(state)


    def _get_baf_instance(self, dev: indigo.Device) -> Optional[BAFDevice]:
        """Helper to find the active connection for either a fan or light device."""
        fan_id = dev.id
        if dev.deviceTypeId == LIGHT_DEVICE_TYPE:
            with self._lock:
                fan_id = self.light_to_fan_map.get(dev.id)
        if fan_id is not None:
            with self._lock:
                return self.fan_to_baf_map.get(fan_id)
        return None


    def _set_device_property(self, dev: indigo.Device, baf_property: str, baf_value: Any) -> None:
        """
        Retrieves the correct BAF instance and sets a property on it in the background.
        """
        baf = self._get_baf_instance(dev)
        if not baf:
            self.logger.error(
                f"Command {baf_property} failed: '{dev.name}' is offline or not linked."
            )

        elif hasattr(baf, baf_property):
            self.logger.debug(
                f"Scheduling set property {baf_property} for {dev.name} to {baf_value}"
            )

            try:
                asyncio.run_coroutine_threadsafe(
                    self._handle_set_baf_device_property(baf, baf_property, baf_value),
                    self.event_loop
                )
            except RuntimeError:
                self.logger.error(
                    f"Failed to schedule property set {baf_property}: event loop is not running."
                )

        else:
            self.logger.error(f"Invalid property: {baf_property} for '{dev.name}'")


    async def _handle_set_baf_device_property(self, baf: BAFDevice, baf_property: str,
                                              baf_value: Any) -> None:
        self.logger.debuf(f"Setting property {baf_property} for {baf.name} to {baf_value}")
        try:
            setattr(baf, baf_property, baf_value)
        except Exception as ex:  # pylint: disable=broad-exception-caught
            self.logger.exception(
                f"Set property {baf_property} for {baf.name} to {baf_value} failed with exception: {ex}"  # pylint: disable=line-too-long
            )


    # --- Fan Action Callbacks ---

    def actionControlSpeedControl(self, action, dev):  # pylint: disable=invalid-name
        """Handles standard Indigo Fan actions (On/Off/Speed)."""
        if dev.deviceTypeId != FAN_DEVICE_TYPE:
            return

        if action.speedControlAction == indigo.kSpeedControlAction.TurnOn:
            self._set_device_property(dev, "fan_mode", OffOnAuto.ON)
        elif action.speedControlAction == indigo.kSpeedControlAction.TurnOff:
            self._set_device_property(dev, "fan_mode", OffOnAuto.OFF)
        elif action.speedControlAction == indigo.kSpeedControlAction.Toggle:
            self._toggle_fan_on_off_state(dev)
        elif action.speedControlAction == indigo.kSpeedControlAction.SetSpeedIndex:
            speed = int(min(action.actionValue / INDIGO_TO_BAF_SPEED_INDEX_RATIO, BAF_SPEED_MAX))
            self._set_device_property(dev, "speed", speed)
        elif action.speedControlAction == indigo.kSpeedControlAction.SetSpeedLevel:
            self._set_device_property(dev, "speed_percent", action.actionValue)
        elif action.speedControlAction == indigo.kSpeedControlAction.IncreaseSpeedIndex:
            self._adjust_fan_speed_index(dev, 1)
        elif action.speedControlAction == indigo.kSpeedControlAction.DecreaseSpeedIndex:
            self._adjust_fan_speed_index(dev, -1)

    # noinspection PyUnusedLocal
    def actionEnableFanAuto(self, action, dev):  # pylint: disable=invalid-name,unused-argument
        """Handles enabling fan auto mode"""
        self._set_device_property(dev, "fan_mode", OffOnAuto.AUTO)

   # noinspection PyUnusedLocal
    def actionEnableWhoosh(self, action, dev):  # pylint: disable=invalid-name,unused-argument
        """Handles enabling fan whoosh mode"""
        self._set_device_property(dev, "whoosh_enable", True)

    # noinspection PyUnusedLocal
    def actionDisableWhoosh(self, action, dev):  # pylint: disable=invalid-name,unused-argument
        """Handles disabling fan whoosh mode"""
        self._set_device_property(dev, "whoosh_enable", False)

    # noinspection PyUnusedLocal
    def actionEnableEco(self, action, dev):  # pylint: disable=invalid-name,unused-argument
        """Handles enabling fan eco mode"""
        self._set_device_property(dev, "eco_enable", True)

    # noinspection PyUnusedLocal
    def actionDisableEco(self, action, dev):  # pylint: disable=invalid-name,unused-argument
        """Handles disabling fan eco mode"""
        self._set_device_property(dev, "eco_enable", False)

    # noinspection PyUnusedLocal
    def actionEnableReverse(self, action, dev):  # pylint: disable=invalid-name,unused-argument
        """Handles enabling fan reverse direction"""
        self._set_device_property(dev, "reverse_enable", True)

    # noinspection PyUnusedLocal
    def actionDisableReverse(self, action, dev):  # pylint: disable=invalid-name,unused-argument
        """Handles disabling fan reverse direction"""
        self._set_device_property(dev, "reverse_enable", False)

    def _toggle_fan_on_off_state(self, dev: indigo.Device) -> None:
        """Handles toggling fan on/off"""
        baf = self._get_baf_instance(dev)
        if baf:
            if baf.fan_mode != OffOnAuto.AUTO:
                new_mode = OffOnAuto.OFF if baf.fan_mode == OffOnAuto.ON else OffOnAuto.ON
                self._set_device_property(dev, "fan_mode", new_mode)
        else:
            self.logger.error(
                f"Command fan toggle failed: '{dev.name}' is offline or not linked."
            )

    def _adjust_fan_speed_index(self, dev: indigo.Device, delta: int) -> None:
        """Handles increasing or decreasing fan speed index"""
        baf = self._get_baf_instance(dev)
        if baf:
            new_speed = int(max(0.0, min(BAF_SPEED_MAX, baf.speed + delta)))
            self._set_device_property(dev, "speed", new_speed)
        else:
            self.logger.error(
                f"Command adjust fan speed failed: '{dev.name}' is offline or not linked."
            )


    # --- Light Action Callbacks ---

    def actionControlDevice(self, action, dev):  # pylint: disable=invalid-name,too-many-branches
        """Handles standard Indigo Device actions (On/Off/Brightness/Color)."""
        if dev.deviceTypeId == FAN_DEVICE_TYPE:
            if action.deviceAction == indigo.kDeviceAction.TurnOn:
                self._set_device_property(dev, "fan_mode", OffOnAuto.ON)
            elif action.deviceAction == indigo.kDeviceAction.TurnOff:
                self._set_device_property(dev, "fan_mode", OffOnAuto.OFF)
            elif action.deviceAction == indigo.kDeviceAction.AllOff:
                self._set_device_property(dev, "fan_mode", OffOnAuto.OFF)
            elif action.deviceAction == indigo.kDeviceAction.Toggle:
                self._toggle_fan_on_off_state(dev)
        elif dev.deviceTypeId == LIGHT_DEVICE_TYPE:
            if action.deviceAction == indigo.kDeviceAction.TurnOn:
                self._set_device_property(dev, "light_mode", OffOnAuto.ON)
            elif action.deviceAction == indigo.kDeviceAction.AllLightsOn:
                self._set_device_property(dev, "light_mode", OffOnAuto.ON)
            elif action.deviceAction == indigo.kDeviceAction.TurnOff:
                self._set_device_property(dev, "light_mode", OffOnAuto.OFF)
            elif action.deviceAction == indigo.kDeviceAction.AllLightsOff:
                self._set_device_property(dev, "light_mode", OffOnAuto.OFF)
            elif action.deviceAction == indigo.kDeviceAction.AllOff:
                self._set_device_property(dev, "light_mode", OffOnAuto.OFF)
            elif action.deviceAction == indigo.kDeviceAction.Toggle:
                self._toggle_light_on_off_state(dev)
            elif action.deviceAction == indigo.kDeviceAction.SetBrightness:
                brightness = int(min(action.actionValue / INDIGO_TO_BAF_BRIGHTNESS_RATIO,
                                     BAF_BRIGHTNESS_MAX))
                self._set_device_property(dev, "light_brightness_level", brightness)
            elif action.deviceAction == indigo.kDeviceAction.BrightenBy:
                self._adjust_light_brightness(dev, action.actionValue)
            elif action.deviceAction == indigo.kDeviceAction.DimBy:
                self._adjust_light_brightness(dev, -1.0 * action.actionValue)
            elif action.deviceAction == indigo.kDeviceAction.SetColorLevels:
                self._set_device_property(dev, "light_color_temperature", action.actionValue)

    # noinspection PyUnusedLocal
    def actionEnableLightAuto(self, action, dev):  # pylint: disable=invalid-name,unused-argument
        """Handles enabling light auto mode"""
        self._set_device_property(dev, "light_mode", OffOnAuto.AUTO)

    def _toggle_light_on_off_state(self, dev: indigo.Device) -> None:
        """Handles toggling light on/off"""
        baf = self._get_baf_instance(dev)
        if baf:
            if baf.light_mode != OffOnAuto.AUTO:
                new_mode = OffOnAuto.OFF if baf.light_mode == OffOnAuto.ON else OffOnAuto.ON
                self._set_device_property(dev, "light_mode", new_mode)
        else:
            self.logger.error(
                f"Command fan toggle failed: '{dev.name}' is offline or not linked."
            )

    def _adjust_light_brightness(self, dev: indigo.Device, delta: int) -> None:
        """Handles increasing or decreasing light brightness"""
        baf = self._get_baf_instance(dev)
        if baf:
            new_brightness = int(max(0.0, min(100.0, baf.light_brightness_percent + delta)))
            self._set_device_property(dev, "light_brightness_level", new_brightness)
        else:
            self.logger.error(
                f"Command adjust fan speed failed: '{dev.name}' is offline or not linked."
            )
