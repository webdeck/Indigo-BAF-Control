# Indigo-BAF-Control

This is a plugin for [Indigo](https://www.indigodomo.com) to control
[Big Ass Fans](https://bigassfans.com) products that use the i6 protocol, which
includes i6 fans and Haiku fans with the 3.0 firmware.

It supports fans with optional lights.  Fans can be added either via
DNS-SD (Bonjour) discovery, or manually by IP address or hostname.

If a fan has a light, a light device will be created along with the fan device.

## Download and Installation

Download from the [Indigo Plugin Store](https://www.indigodomo.com/pluginstore/321/)
and double-click the zip file, and the double click `BAFControl.indigoPlugin`
to install on your Indigo server.

## Configuration

The plugin does not require any configuration other than optioanlly adjusting
the logging level.

To create a new Indigo device, choose the `BAF / Haiku i6 Fan Control` device
type, and choose the `BAF / Haiku Fan` model.  Then either select a discovered
fan from the menu, or manually enter the IP address or hostname.

## Device Control

Fans support all speed control device features, and lights support all dimmer
device features.  A variety of states can be used as triggers for fans and
lights.

TODO: Adding an occupancy sensor device for fans that support that feature.

## Support

Please use the [BAFControl Indigo sub-forum](https://forums.indigodomo.com/viewforum.php?f=422)
for support.

## Acknowledgements

This plugin would not be possible without jfroy's
[aiobafi6 library](https://github.com/jfroy/aiobafi6)
