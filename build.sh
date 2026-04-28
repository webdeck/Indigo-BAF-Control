#!/bin/bash

PLUGIN="BAFControl.indigoPlugin"

rm -fr "$PLUGIN.zip" "$PLUGIN/Contents/Packages" "$PLUGIN/Contents/Server Plugin/__pycache__"
xattr -r -c "$PLUGIN"
python3 /usr/local/indigo/indigo-clean-and-zip-plugin "$PLUGIN"
