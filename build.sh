#!/bin/bash

PLUGIN="BAFControl.indigoPlugin"

rm -f "$PLUGIN.zip"
xattr -r -c "$PLUGIN"
python3 /usr/local/indigo/indigo-clean-and-zip-plugin "$PLUGIN"
