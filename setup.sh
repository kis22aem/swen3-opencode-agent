#!/bin/bash
# setup.sh - Setup script for SWEN v3 agent in opencode

set -e

echo "=== SWEN v3 Agent Setup ==="

# Check Python
echo "Checking Python..."
python3 --version || (echo "Python 3 not found" && exit 1)

# Check zenoh
echo "Checking zenoh..."
python3 -c "import zenoh; print(f'zenoh {zenoh.__version__}')" || {
    echo "Installing zenoh..."
    pip3 install eclipse-zenoh
}

# Create agent directory
echo "Creating agent directory..."
mkdir -p ~/.config/opencode/agents/swen3

# Check if agent files exist
if [ ! -f ~/.config/opencode/agents/swen3/agent.py ]; then
    echo "❌ Agent files not found!"
    echo "Please copy agent files to ~/.config/opencode/agents/swen3/"
    exit 1
fi

# Update opencode.json if needed
if [ -f ~/.config/opencode/opencode.json ]; then
    echo "✅ opencode.json exists"
    
    # Check if swen3 config exists
    if ! grep -q '"swen3"' ~/.config/opencode/opencode.json; then
        echo "Adding SWEN v3 config to opencode.json..."
        # Backup
        cp ~/.config/opencode/opencode.json ~/.config/opencode/opencode.json.bak
        
        # Add swen3 config (simplified - manual edit recommended)
        echo "⚠️  Please manually add SWEN v3 config to opencode.json"
        echo "   See: ~/.config/opencode/agents/swen3/README.md"
    fi
else
    echo "❌ opencode.json not found!"
    exit 1
fi

echo ""
echo "✅ SWEN v3 agent setup complete!"
echo ""
echo "Next steps:"
echo "1. Ensure Diana is accessible: ssh diana-ssh.kolibri-jetson1.uk"
echo "2. Start opencode"
echo "3. Use /swen status to check connection"
echo ""
