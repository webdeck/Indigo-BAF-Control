#! /usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=too-many-lines

"""
Indigo Plugin for Big Ass Fans (BAF) i6/Haiku devices.

This plugin bridges Indigo's synchronous Python environment with the
asynchronous aiobafi6 library using a background asyncio event loop.
Parent Fan device manages child light and/or sensor devices if applicable.
"""

from __future__ import annotations
import asyncio
import concurrent.futures
import logging
from queue import Queue, Empty
import socket
import threading
from typing import Any, Callable, Optional
import indigo  # pylint: disable=import-error
from aiobafi6 import (Device as BAFDevice, Service as BAFService, OffOnAuto)
from device_discovery import BAFDeviceDiscoveryManager, ServiceId

# Typing
DeviceId = int
DeviceMenuItem = tuple[ServiceId, str]
DeviceCreateCallback = Callable[[DeviceId, str, ServiceId, BAFDevice], None]
DeviceUpdateCallback = Callable[[indigo.Device, BAFDevice], None]

# Device Types
FAN_DEVICE_TYPE = "bafFan"
LIGHT_DEVICE_TYPE = "bafLight"
OCCUPANCY_SENSOR_DEVICE_TYPE = "bafOccupancy"
TEMPERATURE_SENSOR_DEVICE_TYPE = "bafTemperature"
HUMIDITY_SENSOR_DEVICE_TYPE = "bafHumidity"

# Property Keys
PROP_PARENT_FAN_ID = "parent_fan_id"
PROP_CHILD_LIGHT_ID = "child_light_id"
PROP_CHILD_OCCUPANCY_SENSOR_ID = "child_occupancy_sensor_id"
PROP_CHILD_TEMPERATURE_SENSOR_ID = "child_temperature_sensor_id"
PROP_CHILD_HUMIDITY_SENSOR_ID = "child_humidity_sensor_id"

# Indigo fan speed index is 0-3; BAF fan speed is 0-7
INDIGO_SPEED_MAX_INDEX = 3.0
BAF_SPEED_MAX = 7.0
INDIGO_TO_BAF_SPEED_INDEX_RATIO = INDIGO_SPEED_MAX_INDEX / BAF_SPEED_MAX

# Service ID for manual IP/Port entry
SERVICE_ID_MANUAL = "manual"
# Default BAF device port
DEFAULT_PORT = 31415


class Plugin(indigo.PluginBase):  # pylint: disable=too-many-public-methods,too-many-instance-attributes
    """Main Plugin class managing communication with BAF/Haiku devices."""


    # --- Plugin Lifecycle ---

    def __init__(self, pluginId: str, pluginDisplayName: str, pluginVersion: str,  # pylint: disable=invalid-name
                 pluginPrefs: dict) -> None:  # pylint: disable=invalid-name
        """Initialize plugin and data structures."""
        super().__init__(pluginId, pluginDisplayName, pluginVersion, pluginPrefs)
        self._set_log_level(pluginPrefs)
        self.temperature_scale = pluginPrefs.get("temperatureScale", "C")

        self._lock = threading.Lock()
        self.service_id_to_baf_map: dict[ServiceId, BAFDevice] = {}
        self.service_id_to_fan_map: dict[ServiceId, list[DeviceId]] = {}
        self.service_id_to_connection_map: dict[ServiceId, asyncio.Future] = {}
        self.fan_availability: dict[DeviceId, bool] = {}

        self.discovery_manager = BAFDeviceDiscoveryManager(self.logger)
        self.event_loop: Optional[asyncio.AbstractEventLoop] = None
        self.event_loop_thread: Optional[threading.Thread] = None
        self._device_operations_queue: Optional[Queue[Callable]] = None
        self._stop_device_ops_event = threading.Event()


    def startup(self) -> None:
        """Called by Indigo wwhen the plugin is enabled."""
        self.logger.info("Starting BAFControl Plugin...")

        # Initialize background loop for asynchronous I/O
        self.event_loop = asyncio.new_event_loop()
        self.event_loop_thread = threading.Thread(
            target=self._run_event_loop,
            daemon=True
        )
        self.event_loop_thread.start()

        self.logger.info("BAFControl Plugin started.")


    def closedPrefsConfigUi(self, valuesDict: dict, userCancelled: bool) -> None:  # pylint: disable=invalid-name
        """Called by Indigo when the plugin preferences dialog is closed"""
        if not userCancelled:
            self._set_log_level(valuesDict)
            new_scale = valuesDict.get("temperatureScale", "C")
            if self.temperature_scale != new_scale:
                self.temperature_scale = new_scale
                with self._lock:
                    baf_devices = list(self.service_id_to_baf_map.values())
                for baf in baf_devices:
                    self._baf_state_callback(baf)


    def runConcurrentThread(self) -> None:  # pylint: disable=invalid-name
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


    def stopConcurrentThread(self) -> None:  # pylint: disable=invalid-name
        """Called by Indigo when the plugin is disabled."""
        self.logger.info("Stopping device operations processor thread...")
        self._stop_device_ops_event.set()


    def shutdown(self) -> None:
        """Called by Indigo when the plugin is disabled."""
        self.logger.info("Shutting down BAFControl Plugin...")

        # Stop discovery manager
        try:
            if self.event_loop and self.event_loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    asyncio.wait_for(self.discovery_manager.stop(), timeout=2.0),
                    self.event_loop
                )
                future.result(timeout=2.5)
        except (asyncio.TimeoutError, concurrent.futures.TimeoutError):
            self.logger.warning("Timed out waiting for discovery to stop.")
        except Exception as ex:  # pylint: disable=broad-exception-caught
            self.logger.exception(f"Error stopping discovery: {ex}")
        finally:
            # Stop all active hardware connections
            with self._lock:
                service_ids = list(self.service_id_to_connection_map.keys())
            for service_id in service_ids:
                self._stop_service_connection(service_id)

            self._stop_event_loop()

            with self._lock:
                self.service_id_to_baf_map.clear()
                self.service_id_to_fan_map.clear()
                self.service_id_to_connection_map.clear()
                self.fan_availability.clear()

            self.logger.info("Plugin shutdown complete.")


    def _add_device_operation(self, op: Callable) -> None:
        """Adds a device operation onto the queue for Indigo's concurrent thread."""
        q = self._device_operations_queue
        if q:
            q.put(op)


    def _run_event_loop(self) -> None:
        """Internal method to start and maintain the asyncio loop."""
        self.logger.debug("Event loop thread starting")
        asyncio.set_event_loop(self.event_loop)

        # Start discovery browser
        self.discovery_manager.start()

        try:
            self.logger.debug("Event loop thread running")
            self.event_loop.run_forever()
        finally:
            self.event_loop.close()
            self.logger.debug("Event loop thread terminating")


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
    def getDiscoveredDevices(self, filter: str = "", valuesDict: Optional[dict] = None,  # pylint: disable=invalid-name,unused-argument,redefined-builtin
                             typeId: str = "", targetId: DeviceId = 0) -> list[DeviceMenuItem]:  # pylint: disable=invalid-name,unused-argument,redefined-builtin
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
    def validateDeviceConfigUi(self, valuesDict: dict, typeId: str,  # pylint: disable=invalid-name,unused-argument
                               devId: DeviceId) -> tuple[bool, dict, indigo.Dict]:  # pylint: disable=invalid-name,unused-argument
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
        return (True, valuesDict, errors)


    # Device Lifecycle

    def deviceStartComm(self, dev: indigo.Device) -> None:  # pylint: disable=invalid-name
        """Called by Indigo when a device is enabled."""
        if dev.deviceTypeId != FAN_DEVICE_TYPE:
            return

        self.logger.debug(f"Starting device communication for device {dev.id}")
        service = self._get_service_from_config(dev.pluginProps)
        if service is None:
            self.logger.error(f"Failed to get device address for device {dev.id}")
            dev.setErrorStateOnServer("invalid configuration")
            return

        service_id = self.discovery_manager.get_service_id(service)

        # Update fan properties
        dev.stateListOrDisplayStateIdChanged()
        props = dict(dev.pluginProps)
        props["supportsAllOff"] = True
        props["supportsStatusRequest"] = False
        dev.replacePluginPropsOnServer(props)

        # Register fan with the service
        with self._lock:
            if service_id not in self.service_id_to_fan_map:
                self.service_id_to_fan_map[service_id] = []
            if dev.id not in self.service_id_to_fan_map[service_id]:
                self.service_id_to_fan_map[service_id].append(dev.id)

            baf = self.service_id_to_baf_map.get(service_id)
            is_running = service_id in self.service_id_to_connection_map

        if is_running:
            # Connection already exists, trigger immediate update for the new device
            if baf:
                self.logger.debug(f"Triggering initial state update for device {dev.id}")
                self._handle_baf_state_callback(baf, dev.id)
        else:
            self.logger.info(
                f"Starting communication with BAF device at {service_id}"
            )
            try:
                with self._lock:
                    self.service_id_to_connection_map[service_id] = \
                        asyncio.run_coroutine_threadsafe(
                            self._start_baf_connection(service_id, service),
                            self.event_loop
                        )
            except Exception as ex:  # pylint: disable=broad-exception-caught
                self.logger.exception(
                    f"Failed to start communication with BAF device at {service_id}: {ex}"
                )
                dev.setErrorStateOnServer("connection failed")


    def deviceStopComm(self, dev: indigo.Device) -> None:  # pylint: disable=invalid-name
        """Called by Indigo when a device is disabled."""
        if dev.deviceTypeId != FAN_DEVICE_TYPE:
            return

        self.logger.debug(f"Stopping device communication for device {dev.id}")
        service_id = dev.address
        stop_service_connection = False
        with self._lock:
            if service_id and service_id in self.service_id_to_fan_map:
                if dev.id in self.service_id_to_fan_map[service_id]:
                    self.service_id_to_fan_map[service_id].remove(dev.id)
                if not self.service_id_to_fan_map[service_id]:
                    # Last Indigo device for this BAF hardware, stop the connection
                    stop_service_connection = True
                    del self.service_id_to_fan_map[service_id]

        if stop_service_connection:
            self._stop_service_connection(service_id)

        self._update_error_state(dev.id, "offline")


    # noinspection PyMethodMayBeStatic
    def didDeviceCommPropertyChange(self, origDev: indigo.Device, newDev: indigo.Device) -> bool:  # pylint: disable=invalid-name
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
        service_id: Optional[ServiceId] = values_dict.get("selected_device")
        if service_id == SERVICE_ID_MANUAL:
            ip_address  = self._get_ip_address_from_config(values_dict)
            port = self._get_port_from_config(values_dict)
            if ip_address and port:
                service = BAFService([ip_address], port)
            else:
                self.logger.error("Manual address and/or port configuration is invalid")
        elif service_id:
            service = self.discovery_manager.get_service_by_id(service_id)

        if service is None:
            address = values_dict.get("address")
            if address:
                service = self.discovery_manager.create_service_from_id(address)

        if service is None:
            self.logger.error(f"Unable to find selected device {service_id}")

        return service


    # Device connection management

    async def _start_baf_connection(self, service_id: ServiceId, service: BAFService) -> None:
        """
        Manages hardware connection. This method runs forever as a background Task.
        """
        self.logger.debug(
            f"Connection established to BAF device at {service_id}"
        )
        backoff = 5.0
        max_backoff = 300.0

        try:
            while True:
                with self._lock:
                    if service_id not in self.service_id_to_connection_map:
                        self.logger.debug(f"Connection to BAF device at {service_id} cancelled.")
                        break
                # noinspection PyBroadException
                try:
                    await self._manage_baf_connection(service_id, service)
                    backoff = 5.0  # Reset backoff on successful connection
                except asyncio.CancelledError:  # pylint:disable=try-except-raise
                    # Propagate cancellation
                    raise
                except Exception as ex:  # pylint: disable=broad-exception-caught
                    self.logger.exception(
                        f"Reconnecting to BAF device at {service_id} in {backoff} seconds: {ex}"
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)

        except asyncio.CancelledError:
            self.logger.debug(
                f"Connection to BAF device at {service_id} was cancelled"
            )
        finally:
            self.logger.debug(
                f"Connection to BAF device at {service_id} closed"
            )


    def _stop_service_connection(self, service_id: ServiceId) -> None:
        """Centralized cleanup for stopping a hardware connection."""
        with self._lock:
            task = self.service_id_to_connection_map.pop(service_id, None)
            self.service_id_to_baf_map.pop(service_id, None)

        if task:
            self.logger.debug(f"Canceling connection to BAF device at {service_id}")
            try:
                task.cancel()
            except Exception as ex:  # pylint: disable=broad-exception-caught
                self.logger.exception(
                    f"Failed to cancel connection to BAF device at {service_id}: {ex}"
                )


    async def _manage_baf_connection(self, service_id: ServiceId,
                                     service: BAFService) -> None:
        """
        Connects to a fan device and processes state callbacks.
        Keeps running for as long as the fan stays connected.
        """
        self.logger.debug(
            f"Attempting connection to BAF device at {service_id}"
        )

        baf = BAFDevice(service)
        with self._lock:
            self.service_id_to_baf_map[service_id] = baf

        baf.add_callback(self._baf_state_callback)

        self.logger.info(
            f"Connection established to BAF device at {service_id}; monitoring connection..."
        )

        try:
            await baf.async_run()
        finally:
            baf.remove_callback(self._baf_state_callback)
            self.logger.warning(f"Connection closed to BAF device at {service_id}")
            self._update_error_state_for_service_id(service_id, "offline")


    def _baf_state_callback(self, baf_device: BAFDevice) -> None:
        """Handles callback from all BAF devices"""
        self.logger.debug(f"Callback received for fan {baf_device.name}")
        fan_ids = self._get_fan_ids_from_baf_device(baf_device)
        if fan_ids:
            for fan_id in fan_ids:
                self._handle_baf_state_callback(baf_device, fan_id)
        else:
            self.logger.debug(f"Unable to find Indigo device for BAF device {baf_device.name}")


    def _handle_baf_state_callback(self, baf_device: BAFDevice, fan_id: DeviceId) -> None:
        """Handles callback for a specific BAF device with state updates"""
        try:
            fan_dev: Optional[indigo.Device] = indigo.devices.get(fan_id)
            if not fan_dev:
                self.logger.debug(f"Fan device {fan_id} no longer exists, skipping callback")
                return

            self._update_fan_states(fan_dev, baf_device)

            self._update_child_device(fan_dev, baf_device, PROP_CHILD_LIGHT_ID,
                                      baf_device.has_light, self._create_light,
                                      self._update_light_states)

            self._update_child_device(fan_dev, baf_device, PROP_CHILD_OCCUPANCY_SENSOR_ID,
                                      baf_device.has_occupancy, self._create_occupancy_sensor,
                                      self._update_occupancy_sensor_states)

            self._update_child_device(fan_dev, baf_device, PROP_CHILD_TEMPERATURE_SENSOR_ID,
                                      baf_device.temperature is not None,
                                      self._create_temperature_sensor,
                                      self._update_temperature_sensor_states)

            self._update_child_device(fan_dev, baf_device, PROP_CHILD_HUMIDITY_SENSOR_ID,
                                      baf_device.humidity is not None,
                                      self._create_humidity_sensor,
                                      self._update_humidity_sensor_states)

        except Exception as ex:  # pylint: disable=broad-exception-caught
            self.logger.exception(f"State callback error for fan {fan_id}: {ex}")


    def _update_child_device(self, fan_dev: indigo.Device, baf_device: BAFDevice,  # pylint: disable=too-many-arguments,too-many-positional-arguments
                             child_id_property_key: str, child_device_exists: bool,
                             create_callback: DeviceCreateCallback,
                             update_callback: DeviceUpdateCallback) -> None:
        """Helper method to create/update a child device.  Child devices are never deleted."""
        child_dev: Optional[indigo.Device] = None
        child_id: Optional[DeviceId] = fan_dev.pluginProps.get(child_id_property_key)
        if child_id:
            child_dev = indigo.devices.get(child_id)
            if child_dev:
                self._update_device_available(child_dev, child_device_exists)

        if child_device_exists:
            if child_dev:
                if child_dev.address != fan_dev.address:
                    new_props = child_dev.pluginProps
                    new_props["address"] = fan_dev.address
                    self._add_device_operation(
                        lambda: child_dev.replacePluginPropsOnServer(new_props)
                    )
                    update_callback(child_dev, baf_device)
            else:
                self.logger.debug(f"Queueing child device creation for fan {fan_dev.id}")
                self._add_device_operation(
                    lambda: create_callback(fan_dev.id, fan_dev.name, fan_dev.address, baf_device)
                )


    def _create_light(self, fan_id: DeviceId, fan_name: str,
                      service_id: ServiceId, baf_device: BAFDevice) -> None:
        """Helper method to create a child light device (called on concurrent thread)."""
        fan_dev: Optional[indigo.Device] = indigo.devices.get(fan_id)
        if fan_dev and not fan_dev.pluginProps.get(PROP_CHILD_LIGHT_ID):
            new_light: indigo.Device = indigo.device.create(
                protocol=indigo.kProtocol.Plugin,
                address=service_id,
                name=f"{fan_name} Light",
                deviceTypeId=LIGHT_DEVICE_TYPE,
                folder=fan_dev.folderId
            )
            new_light.configured = True
            new_light.replaceOnServer()
            indigo.device.groupWithDevice(new_light, fan_dev)

            # Update light properties
            props = dict(new_light.pluginProps)
            supports_color_temp = (baf_device.light_warmest_color_temperature !=
                                   baf_device.light_coolest_color_temperature)
            props["SupportsAllLightsOnOff"] = True
            props["SupportsAllOff"] = True
            props["SupportsColor"] = supports_color_temp
            props["SupportsRGB"] = False
            props["SupportsRGBandWhiteSimultaneously"] = False
            props["SupportsStatusRequest"] = False
            props["SupportsTwoWhiteLevels"] = False
            props["SupportsTwoWhiteLevelsSimultaneously"] = False
            props["SupportsWhite"] = supports_color_temp
            props["SupportsWhiteTemperature"] = supports_color_temp
            props["WhiteTemperatureMin"] = baf_device.light_warmest_color_temperature
            props["WhiteTemperatureMax"] = baf_device.light_coolest_color_temperature
            props[PROP_PARENT_FAN_ID] = fan_dev.id
            new_light.replacePluginPropsOnServer(props)

            # Update fan properties
            props = dict(fan_dev.pluginProps)
            props[PROP_CHILD_LIGHT_ID] = new_light.id
            fan_dev.replacePluginPropsOnServer(props)

            self.logger.info(f"Created child light device {new_light.id} for {fan_name}")

            self._update_light_states(new_light, baf_device)


    def _create_occupancy_sensor(self, fan_id: DeviceId, fan_name: str,
                                 service_id: ServiceId, baf_device: BAFDevice) -> None:
        """Helper method to create a child occupancy sensor device (called on concurrent thread)."""
        fan_dev: Optional[indigo.Device] = indigo.devices.get(fan_id)
        if fan_dev and not fan_dev.pluginProps.get(PROP_CHILD_OCCUPANCY_SENSOR_ID):
            new_sensor: indigo.Device = indigo.device.create(
                protocol=indigo.kProtocol.Plugin,
                address=service_id,
                name=f"{fan_name} Occupancy Sensor",
                deviceTypeId=OCCUPANCY_SENSOR_DEVICE_TYPE,
                folder=fan_dev.folderId
            )
            new_sensor.configured = True
            new_sensor.replaceOnServer()
            indigo.device.groupWithDevice(new_sensor, fan_dev)

            # Update sensor properties
            props = dict(new_sensor.pluginProps)
            props["AllowOnStateChange"] = False
            props["AllowSensorValueChange"] = False
            props["SupportsOnState"] = True
            props["SupportsSensorValue"] = False
            props["SupportsStatusRequest"] = False
            props[PROP_PARENT_FAN_ID] = fan_dev.id
            new_sensor.replacePluginPropsOnServer(props)

            # Update fan properties
            props = dict(fan_dev.pluginProps)
            props[PROP_CHILD_OCCUPANCY_SENSOR_ID] = new_sensor.id
            fan_dev.replacePluginPropsOnServer(props)

            self.logger.info(
                f"Created child occupancy sensor device {new_sensor.id} for {fan_name}"
            )

            self._update_occupancy_sensor_states(new_sensor, baf_device)


    def _create_temperature_sensor(self, fan_id: DeviceId, fan_name: str,
                                   service_id: ServiceId, baf_device: BAFDevice) -> None:
        """Helper method to create child temperature sensor device (called on concurrent thread)."""
        fan_dev: Optional[indigo.Device] = indigo.devices.get(fan_id)
        if fan_dev and not fan_dev.pluginProps.get(PROP_CHILD_TEMPERATURE_SENSOR_ID):
            new_sensor: indigo.Device = indigo.device.create(
                protocol=indigo.kProtocol.Plugin,
                address=service_id,
                name=f"{fan_name} Temperature Sensor",
                deviceTypeId=TEMPERATURE_SENSOR_DEVICE_TYPE,
                folder=fan_dev.folderId
            )
            new_sensor.configured = True
            new_sensor.replaceOnServer()
            indigo.device.groupWithDevice(new_sensor, fan_dev)

            # Update sensor properties
            props = dict(new_sensor.pluginProps)
            props["AllowOnStateChange"] = False
            props["AllowSensorValueChange"] = False
            props["SupportsOnState"] = False
            props["SupportsSensorValue"] = True
            props["SupportsStatusRequest"] = False
            props[PROP_PARENT_FAN_ID] = fan_dev.id
            new_sensor.replacePluginPropsOnServer(props)

            # Update fan properties
            props = dict(fan_dev.pluginProps)
            props[PROP_CHILD_TEMPERATURE_SENSOR_ID] = new_sensor.id
            fan_dev.replacePluginPropsOnServer(props)

            self.logger.info(
                f"Created child temperature sensor device {new_sensor.id} for {fan_name}"
            )

            self._update_temperature_sensor_states(new_sensor, baf_device)


    def _create_humidity_sensor(self, fan_id: DeviceId, fan_name: str,
                                  service_id: ServiceId, baf_device: BAFDevice) -> None:
        """Helper method to create child humidity sensor device (called on concurrent thread)."""
        fan_dev: Optional[indigo.Device] = indigo.devices.get(fan_id)
        if fan_dev and not fan_dev.pluginProps.get(PROP_CHILD_HUMIDITY_SENSOR_ID):
            new_sensor: indigo.Device = indigo.device.create(
                protocol=indigo.kProtocol.Plugin,
                address=service_id,
                name=f"{fan_name} Humidity Sensor",
                deviceTypeId=HUMIDITY_SENSOR_DEVICE_TYPE,
                folder=fan_dev.folderId
            )
            new_sensor.configured = True
            new_sensor.replaceOnServer()
            indigo.device.groupWithDevice(new_sensor, fan_dev)

            # Update sensor properties
            props = dict(new_sensor.pluginProps)
            props["AllowOnStateChange"] = False
            props["AllowSensorValueChange"] = False
            props["SupportsOnState"] = False
            props["SupportsSensorValue"] = True
            props["SupportsStatusRequest"] = False
            props[PROP_PARENT_FAN_ID] = fan_dev.id
            new_sensor.replacePluginPropsOnServer(props)

            # Update fan properties
            props = dict(fan_dev.pluginProps)
            props[PROP_CHILD_HUMIDITY_SENSOR_ID] = new_sensor.id
            fan_dev.replacePluginPropsOnServer(props)

            self.logger.info(
                f"Created child humidity sensor device {new_sensor.id} for {fan_name}"
            )

            self._update_humidity_sensor_states(new_sensor, baf_device)


    def _update_fan_states(self, fan_dev: indigo.Device, baf_dev: BAFDevice) -> None:
        """Maps BAF properties to native and custom Indigo device states."""
        self._update_device_available(fan_dev, baf_dev.available)
        if baf_dev.available:
            speed_index = int(round(min(baf_dev.speed * INDIGO_TO_BAF_SPEED_INDEX_RATIO,
                                    INDIGO_SPEED_MAX_INDEX)))
            on_off_state = baf_dev.speed > 0
            auto_mode = baf_dev.fan_mode == OffOnAuto.AUTO
            comfort_ideal_temperature = self._scale_temperature(baf_dev.comfort_ideal_temperature)
            states = [
                {'key': 'speedIndex', 'value': speed_index},
                {'key': 'speedLevel', 'value': baf_dev.speed_percent},
                {'key': 'onOffState', 'value': on_off_state},
                {'key': 'baf_speed', 'value': baf_dev.speed},
                {'key': 'auto_mode', 'value': auto_mode},
                {'key': 'whoosh_mode', 'value': baf_dev.whoosh_enable},
                {'key': 'eco_mode', 'value': baf_dev.eco_enable},
                {'key': 'reverse_direction', 'value': baf_dev.reverse_enable},
                {'key': 'has_auto_comfort', 'value': baf_dev.has_auto_comfort},
                {'key': 'has_occupancy', 'value': baf_dev.has_occupancy},
                {'key': 'auto_comfort', 'value': baf_dev.auto_comfort_enable},
                {'key': 'comfort_ideal_temperature',
                 'value': comfort_ideal_temperature,
                 'uiValue': f"{comfort_ideal_temperature:.1f} °{self.temperature_scale}",
                 'decimalPlaces': 1},
                {'key': 'comfort_heat_assist_enable', 'value': baf_dev.comfort_heat_assist_enable},
                {'key': 'comfort_heat_assist_speed', 'value': baf_dev.comfort_heat_assist_speed},
                {'key': 'comfort_heat_assist_reverse',
                 'value': baf_dev.comfort_heat_assist_reverse_enable},
                {'key': 'comfort_min_speed', 'value': baf_dev.comfort_min_speed},
                {'key': 'comfort_max_speed', 'value': baf_dev.comfort_max_speed},
                {'key': 'motion_sense', 'value': baf_dev.motion_sense_enable},
                {'key': 'motion_sense_timeout', 'value': baf_dev.motion_sense_timeout},
                {'key': 'return_to_auto', 'value': baf_dev.return_to_auto_enable},
                {'key': 'return_to_auto_timeout', 'value': baf_dev.return_to_auto_timeout},
                {'key': 'target_rpm', 'value': baf_dev.target_rpm},
                {'key': 'device_name', 'value': baf_dev.name},
                {'key': 'ip_address', 'value': baf_dev.ip_address},
                {'key': 'led_indicators_enabled', 'value': baf_dev.led_indicators_enable},
                {'key': 'beep_enabled', 'value': baf_dev.fan_beep_enable},
                {'key': 'legacy_ir_remote_enabled', 'value': baf_dev.legacy_ir_remote_enable}

            ]
            self._add_optional_property(states, 'model', baf_dev.model)
            self._add_optional_property(states, 'firmware_version', baf_dev.firmware_version)
            self._add_optional_property(states, 'mac_address', baf_dev.mac_address)
            self._add_optional_property(states, 'dns_sd_uuid', baf_dev.dns_sd_uuid)
            self._add_optional_property(states, 'api_version', baf_dev.api_version)
            self._add_optional_property(states, 'has_light', baf_dev.has_light)
            self._add_optional_property(states, 'wifi_ssid', baf_dev.wifi_ssid)
            self._add_device_operation(
                lambda: self._update_device_states_on_server(fan_dev, states)
            )


    @staticmethod
    def _add_optional_property(states: list, key: str, value: Optional[Any]) -> None:
        """Adds a key-value pair to the states list if it is not None"""
        if value is not None:
            states.append({'key': key, 'value': value})


    def _update_device_states_on_server(self, dev: indigo.Device,
                                        states: list[indigo.Dict]) -> None:
        """Updates the device states on the server (called on concurrent thread)."""
        try:
            dev.updateStatesOnServer(states)
        except (KeyError, AttributeError, TypeError) as ex:
            self.logger.exception(f"State update failed for {dev.name}: {ex}")


    def _update_light_states(self, light_dev: indigo.Device, baf_dev: BAFDevice) -> None:
        """Maps BAF properties to native and custom Indigo device states."""
        self._update_device_available(light_dev, baf_dev.available)
        if baf_dev.available:
            on_off_state = baf_dev.light_brightness_percent > 0
            auto_mode = baf_dev.light_mode == OffOnAuto.AUTO
            states = [
                indigo.Dict({'key': 'onOffState',
                             'value': on_off_state}),
                indigo.Dict({'key': 'brightnessLevel',
                             'value': baf_dev.light_brightness_percent}),
                indigo.Dict({'key': 'brightness_index',
                             'value': baf_dev.light_brightness_level}),
                indigo.Dict({'key': 'auto_mode',
                             'value': auto_mode}),
                indigo.Dict({'key': 'dim_to_warm',
                             'value': baf_dev.light_dim_to_warm_enable}),
                indigo.Dict({'key': 'auto_motion_timeout',
                             'value': baf_dev.light_auto_motion_timeout}),
                indigo.Dict({'key': 'return_to_auto',
                             'value': baf_dev.light_return_to_auto_enable}),
                indigo.Dict({'key': 'return_to_auto_timeout',
                             'value': baf_dev.light_return_to_auto_timeout})
            ]
            if light_dev.pluginProps.get("SupportsWhiteTemperature", False) is True:
                states.append(indigo.Dict({'key': 'whiteTemperature',
                                           'value': baf_dev.light_color_temperature}))
            self._add_device_operation(
                lambda: self._update_device_states_on_server(light_dev, states)
            )


    def _update_occupancy_sensor_states(self, sensor_dev: indigo.Device,
                                        baf_dev: BAFDevice) -> None:
        """Maps BAF properties to native and custom Indigo device states."""
        self._update_device_available(sensor_dev, baf_dev.available)
        if baf_dev.available:
            on_off_state = baf_dev.fan_occupancy_detected or baf_dev.light_occupancy_detected
            self._add_device_operation(
                lambda: self._update_sensor_states_on_server(sensor_dev, on_off_state)
            )


    def _update_sensor_states_on_server(self, sensor_dev: indigo.Device,
                                        on_off_state: bool) -> None:
        """Updates the sensor device states on the server (called on concurrent thread)."""
        states = [
            indigo.Dict({'key': 'onOffState', 'value': on_off_state})
        ]
        self._update_device_states_on_server(sensor_dev, states)

        if on_off_state:
            sensor_dev.updateStateImageOnServer(indigo.kStateImageSel.MotionSensorTripped)
        else:
            sensor_dev.updateStateImageOnServer(indigo.kStateImageSel.MotionSensor)


    def _update_temperature_sensor_states(self, sensor_dev: indigo.Device,
                                          baf_dev: BAFDevice) -> None:
        """Maps BAF properties to native and custom Indigo device states."""
        self._update_device_available(sensor_dev, baf_dev.available)
        if baf_dev.available:
            temperature = self._scale_temperature(baf_dev.temperature)
            states = []
            if temperature is not None:
                states.append(indigo.Dict(
                    {'key': 'sensorValue',
                     'value': temperature,
                     'uiValue': f"{temperature:.1f} °{self.temperature_scale}",
                     'decimalPlaces': 1}
                ))
            else:
                states.append(indigo.Dict({'key': 'sensorValue', 'value': None}))

            self._add_device_operation(
                lambda: self._update_device_states_on_server(sensor_dev, states)
            )


    def _scale_temperature(self, temperature: Optional[float]) -> Optional[float]:
        """Converts temperature to appropriate scale"""
        result = temperature
        if temperature is not None and self.temperature_scale == "F":
            result = temperature * 9.0 / 5.0 + 32.0
        return result


    def _update_humidity_sensor_states(self, sensor_dev: indigo.Device,
                                          baf_dev: BAFDevice) -> None:
        """Maps BAF properties to native and custom Indigo device states."""
        self._update_device_available(sensor_dev, baf_dev.available)
        if baf_dev.available:
            humidity = baf_dev.humidity
            states = []
            if humidity is not None:
                states.append(indigo.Dict(
                    {'key': 'sensorValue',
                     'value': humidity,
                     'uiValue': f"{humidity:.0f}%",
                     'decimalPlaces': 0}
                ))
            else:
                states.append(indigo.Dict({'key': 'sensorValue', 'value': None}))

            self._add_device_operation(
                lambda: self._update_device_states_on_server(sensor_dev, states)
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
        """Updates the error state for the fan device (and child devices if applicable)"""
        with self._lock:
            self.fan_availability[fan_id] = state is None
        fan = indigo.devices.get(fan_id)
        if fan:
            fan.setErrorStateOnServer(state)
            self._update_error_state_child_device(fan, PROP_CHILD_LIGHT_ID, state)
            self._update_error_state_child_device(fan, PROP_CHILD_OCCUPANCY_SENSOR_ID, state)
            self._update_error_state_child_device(fan, PROP_CHILD_TEMPERATURE_SENSOR_ID, state)
            self._update_error_state_child_device(fan, PROP_CHILD_HUMIDITY_SENSOR_ID, state)


    @staticmethod
    def _update_error_state_child_device(dev: indigo.Device, child_device_property_key: str,
                                         state: Optional[str]) -> None:
        child_dev_id = dev.pluginProps.get(child_device_property_key)
        if child_dev_id:
            child_dev = indigo.devices.get(child_dev_id)
            if child_dev:
                child_dev.setErrorStateOnServer(state)


    def _update_error_state_for_service_id(self, service_id: ServiceId,
                                           state: Optional[str]) -> None:
        """Updates the error state for all Indigo devices associated with a BAF device"""
        with self._lock:
            fan_ids = list(self.service_id_to_fan_map.get(service_id, []))
        for fan_id in fan_ids:
            self._update_error_state(fan_id, state)


    def _get_fan_ids_from_baf_device(self, baf_dev: BAFDevice) -> list[DeviceId]:
        """Helper to find the Indigo fan devices for a BAF device."""
        service_id = self.discovery_manager.get_service_id(baf_dev.service)
        with self._lock:
            return list(self.service_id_to_fan_map.get(service_id, []))


    def _get_baf_instance(self, dev: indigo.Device) -> Optional[BAFDevice]:
        """Helper to find the active connection for either a fan or child device."""
        service_id = dev.address
        if service_id:
            with self._lock:
                return self.service_id_to_baf_map.get(service_id)
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
        self.logger.debug(f"Setting property {baf_property} for {baf.name} to {baf_value}")
        try:
            setattr(baf, baf_property, baf_value)
        except Exception as ex:  # pylint: disable=broad-exception-caught
            self.logger.exception(
                f"Set property {baf_property} for {baf.name} to {baf_value} failed with exception: {ex}"  # pylint: disable=line-too-long
            )


    # --- Action Configuration Callbacks ---

    def validateActionConfigUi(self, valuesDict: indigo.Dict, typeId: str,  # pylint: disable=invalid-name,unused-argument
                               devId: DeviceId) -> tuple[bool, dict, indigo.Dict]:  # pylint: disable=invalid-name,unused-argument
        """Called by Indigo to validate the action configuration"""
        errors = indigo.Dict()

        if typeId == "setBAFFanSpeed":
            speed_str = valuesDict.get("speed")
            if speed_str:
                try:
                    speed = int(speed_str)
                    if 0 <= speed <= BAF_SPEED_MAX:
                        # noinspection PyRedundantParentheses
                        return (True, valuesDict, errors)
                except ValueError:
                    pass
            errors["speed"] = "Invalid speed specified"
            # noinspection PyRedundantParentheses
            return (False, valuesDict, errors)

        # noinspection PyRedundantParentheses
        return (True, valuesDict, errors)


    # --- Fan Action Callbacks ---

    def actionControlSpeedControl(self, action: indigo.SpeedControlAction,  # pylint: disable=invalid-name
                                  dev: indigo.Device) -> None:
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
            speed = int(round(min(action.actionValue / INDIGO_TO_BAF_SPEED_INDEX_RATIO,
                                  BAF_SPEED_MAX)))
            self._set_device_property(dev, "speed", speed)
        elif action.speedControlAction == indigo.kSpeedControlAction.SetSpeedLevel:
            speed = int(round(min(action.actionValue / 100.0 * BAF_SPEED_MAX,
                                  BAF_SPEED_MAX)))
            self._set_device_property(dev, "speed", speed)
        elif action.speedControlAction == indigo.kSpeedControlAction.IncreaseSpeedIndex:
            self._adjust_fan_speed_index(dev, 1)
        elif action.speedControlAction == indigo.kSpeedControlAction.DecreaseSpeedIndex:
            self._adjust_fan_speed_index(dev, -1)

    # noinspection PyUnusedLocal
    def actionSetBAFFanSpeed(self, action: indigo.BaseAction, dev: indigo.Device) -> None:  # pylint: disable=invalid-name,unused-argument
        """Handles setting fan speed using BAF 0-7 range"""
        self._set_device_property(dev, "speed", int(action.props.get("speed", "0")))

    # noinspection PyUnusedLocal
    def actionEnableFanAuto(self, action: indigo.BaseAction, dev: indigo.Device) -> None:  # pylint: disable=invalid-name,unused-argument
        """Handles enabling fan auto mode"""
        self._set_device_property(dev, "fan_mode", OffOnAuto.AUTO)

   # noinspection PyUnusedLocal
    def actionEnableWhoosh(self, action: indigo.BaseAction, dev: indigo.Device) -> None:  # pylint: disable=invalid-name,unused-argument
        """Handles enabling fan whoosh mode"""
        self._set_device_property(dev, "whoosh_enable", True)

    # noinspection PyUnusedLocal
    def actionDisableWhoosh(self, action: indigo.BaseAction, dev: indigo.Device) -> None:  # pylint: disable=invalid-name,unused-argument
        """Handles disabling fan whoosh mode"""
        self._set_device_property(dev, "whoosh_enable", False)

    # noinspection PyUnusedLocal
    def actionEnableEco(self, action: indigo.BaseAction, dev: indigo.Device) -> None:  # pylint: disable=invalid-name,unused-argument
        """Handles enabling fan eco mode"""
        self._set_device_property(dev, "eco_enable", True)

    # noinspection PyUnusedLocal
    def actionDisableEco(self, action: indigo.BaseAction, dev: indigo.Device) -> None:  # pylint: disable=invalid-name,unused-argument
        """Handles disabling fan eco mode"""
        self._set_device_property(dev, "eco_enable", False)

    # noinspection PyUnusedLocal
    def actionEnableReverse(self, action: indigo.BaseAction, dev: indigo.Device) -> None:  # pylint: disable=invalid-name,unused-argument
        """Handles enabling fan reverse direction"""
        self._set_device_property(dev, "reverse_enable", True)

    # noinspection PyUnusedLocal
    def actionDisableReverse(self, action: indigo.BaseAction, dev: indigo.Device) -> None:  # pylint: disable=invalid-name,unused-argument
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

    def actionControlDevice(self, action: indigo.DeviceAction, dev: indigo.Device) -> None:  # pylint: disable=invalid-name,too-many-branches
        """Handles standard Indigo Device actions (On/Off/Brightness/Color)."""
        if dev.deviceTypeId == FAN_DEVICE_TYPE:
            if action.deviceAction == indigo.kDeviceAction.TurnOn:
                self._set_device_property(dev, "fan_mode", OffOnAuto.ON)
            elif action.deviceAction in (indigo.kDeviceAction.TurnOff,
                                         indigo.kDeviceAction.AllOff):
                self._set_device_property(dev, "fan_mode", OffOnAuto.OFF)
            elif action.deviceAction == indigo.kDeviceAction.Toggle:
                self._toggle_fan_on_off_state(dev)
        elif dev.deviceTypeId == LIGHT_DEVICE_TYPE:
            if action.deviceAction in (indigo.kDeviceAction.TurnOn,
                                       indigo.kDeviceAction.AllLightsOn):
                self._set_device_property(dev, "light_mode", OffOnAuto.ON)
            elif action.deviceAction in (indigo.kDeviceAction.TurnOff,
                                         indigo.kDeviceAction.AllLightsOff,
                                         indigo.kDeviceAction.AllOff):
                self._set_device_property(dev, "light_mode", OffOnAuto.OFF)
            elif action.deviceAction == indigo.kDeviceAction.Toggle:
                self._toggle_light_on_off_state(dev)
            elif action.deviceAction == indigo.kDeviceAction.SetBrightness:
                self._set_device_property(dev, "light_brightness_percent", action.actionValue)
            elif action.deviceAction == indigo.kDeviceAction.BrightenBy:
                self._adjust_light_brightness(dev, action.actionValue)
            elif action.deviceAction == indigo.kDeviceAction.DimBy:
                self._adjust_light_brightness(dev, -1.0 * action.actionValue)
            elif action.deviceAction == indigo.kDeviceAction.SetColorLevels:
                color_dict: dict = action.actionValue
                if 'whiteTemperature' in color_dict:
                    self._set_device_property(dev, "light_color_temperature",
                                              color_dict['whiteTemperature'])

    # noinspection PyUnusedLocal
    def actionEnableLightAuto(self, action: indigo.BaseAction, dev: indigo.Device) -> None:  # pylint: disable=invalid-name,unused-argument
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
            self._set_device_property(dev, "light_brightness_percent", new_brightness)
        else:
            self.logger.error(
                f"Command adjust light brightness failed: '{dev.name}' is offline or not linked."
            )
