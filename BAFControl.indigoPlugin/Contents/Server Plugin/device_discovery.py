#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
Device discovery module for BAF/Haiku fans.
"""

from __future__ import annotations
import logging
import threading
from typing import Optional
from aiobafi6 import ServiceBrowser, Service
from zeroconf.asyncio import AsyncZeroconf

# Typing
ServiceId = str


class BAFDeviceDiscoveryManager:
    """Manages device discovery for BAF devices using ServiceBrowser."""

    def __init__(self, logger: logging.Logger) -> None:
        """Initialize the discovery manager with a reference to the logger to use."""
        self.logger = logger
        self.azc: Optional[AsyncZeroconf] = None
        self.browser: Optional[ServiceBrowser] = None
        self._discovered_services: dict[ServiceId, Service] = {}
        self._lock = threading.Lock()


    # Lifecycle management

    async def start(self) -> None:
        """Starts device discovery."""
        if not self.azc:
            self.logger.debug("Starting BAF device discovery")
            # noinspection PyBroadException
            try:
                self.azc = AsyncZeroconf()
                # noinspection PyTypeChecker
                self.browser = ServiceBrowser(self.azc.zeroconf, self)
            except Exception:  # pylint: disable=broad-exception-caught
                self.logger.exception("Failed to create AsyncZeroconf")


    async def stop(self) -> None:
        """Stops device discovery."""
        if self.azc:
            self.logger.debug("Stopping BAF device discovery")
            # noinspection PyBroadException
            try:
                await self.azc.async_close()
            except Exception:  # pylint: disable=broad-exception-caught
                pass
            self.azc = None
            self.browser = None


    # Service management

    @staticmethod
    def get_service_id(service: Service) -> Optional[ServiceId]:
        """Extract service ID from a Service object in the form {ip_address}:{port}"""
        if (service and hasattr(service, 'ip_addresses') and
                service.ip_addresses and len(service.ip_addresses) > 0):
            ip_address = service.ip_addresses[0]
            port = service.port
            if ip_address and port:
                return f"{ip_address}:{port}"
        return None


    def get_service_by_id(self, service_id: ServiceId) -> Optional[Service]:
        """Retrieve a service by its ID."""
        with self._lock:
            return self._discovered_services.get(service_id)


    def add_service_by_id(self, service: Service, service_id: ServiceId) -> None:
        """Add a service by its ID."""
        with self._lock:
            self._discovered_services[service_id] = service


    def remove_service_by_id(self, service_id: ServiceId) -> None:
        """Remove a service by its ID."""
        with self._lock:
            if service_id in self._discovered_services:
                del self._discovered_services[service_id]


    def clear_discovered_services(self) -> None:
        """Clear all discovered fans."""
        with self._lock:
            self._discovered_services.clear()


    @property
    def discovered_services(self) -> dict[ServiceId, Service]:
        """Get a copy of the current discovered fans dictionary."""
        with self._lock:
            return dict(self._discovered_services.items())


    # Internal ServiceBrowser callbacks

    def add_service(self, service: Service) -> None:
        """Callback from aiobafi6.discovery when a verified fan is found."""
        self.logger.info(
            f"Discovered BAF Device: {service.device_name} {service.ip_addresses} {service.port}"
        )
        service_id = self.get_service_id(service)
        if service_id:
            self.logger.debug(f"Saving BAF Device with id {service_id}")
            self.add_service_by_id(service, service_id)
        else:
            self.logger.warning(
                f"Unable to get service ID for {service.device_name} {service.ip_addresses} {service.port}"  # pylint: disable=line-too-long
            )

    def remove_service(self, service: Service) -> None:
        """Callback from aiobafi6.discovery when a fan is removed."""
        self.logger.info(
            f"Removed BAF Device: {service.device_name} {service.ip_addresses} {service.port}"
        )
        service_id = self.get_service_id(service)
        if service_id:
            self.logger.debug(f"Removing BAF Device with id {service_id}")
            self.remove_service_by_id(service_id)
        else:
            self.logger.warning(
                f"Unable to get service ID for {service.device_name} {service.ip_addresses} {service.port}"  # pylint: disable=line-too-long
            )
