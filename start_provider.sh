#!/bin/bash
# Start SwarmProvider v3

echo "🚀 Starting SwarmProvider v3..."
echo "   Port: 8080"
echo "   Workers: qwen3_5_4b_opus, jetson_gemma4b"
echo ""

source ~/.venvs/swen3/bin/activate

python3 -c "
import sys, os
sys.path.insert(0, os.path.expanduser('~/.local/share/swen3'))
from swarm_provider import app, get_backend
import uvicorn

print('Connecting to Zenoh mesh...')
backend = get_backend()
backend.connect()
print('✅ Connected!')
print('Starting server...')
uvicorn.run(app, host='0.0.0.0', port=8080, log_level='info')
"
