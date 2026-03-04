Indigo-BAF-Control

This project implements an Indigo plugin to integrate with BAF/Haiku fans and lights.

The Indigo Plugin SDK documentation is here:
https://wiki.indigodomo.com/doku.php?id=indigo_2025.1_documentation:plugin_guide
https://github.com/IndigoDomotics/IndigoSDK

This plugin uses the aiobaf6 library to communicate with BAF/Haiku fans and lights:
https://github.com/jfroy/aiobafi6/tree/main to connect to BAF/Haiku fans.

Each fan can have an optional light.  The light would be a "sub-device" associated with the fan.

Further examples of Indigo Plugins using the SDK can be found here:
https://github.com/FlyingDiver/Indigo-HA-Agent
https://github.com/Ghawken/HomeKitLink-Siri
https://github.com/FlyingDiver/Indigo-BondHome
https://github.com/autolog/Starling_Bridge
