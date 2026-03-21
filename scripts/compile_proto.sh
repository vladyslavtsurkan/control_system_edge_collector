#!/bin/bash
# Create the output directory if it doesn't exist
mkdir -p app/generated
touch app/generated/__init__.py

# Generate Python classes from the telemetry.proto file
python -m grpc_tools.protoc \
    -I contracts \
    --python_out=app/generated \
    contracts/telemetry.proto

echo "Protobuf classes generated successfully in app/generated/ directory."
