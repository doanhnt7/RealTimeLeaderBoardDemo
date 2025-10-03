#!/bin/bash

# Start Jupyter Lab
echo "Starting Jupyter Lab..."
echo "Access Jupyter Lab at: http://localhost:8888"
echo "Default password: spark"

# Start Jupyter Lab with no browser and allow all IPs
jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.disable_check_xsrf=True
