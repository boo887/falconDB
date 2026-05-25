#!/bin/bash

echo "Stopping pod falcondb..."
podman pod stop falcondb
podman pod rm falcondb

echo "Done."
