#!/bin/bash
# =============================================================================
# JobScout - Configuration Validation Script
# =============================================================================
# Checks that your environment is properly configured before running JobScout
# Run this after initial setup to catch issues early
# =============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

ERRORS=0
WARNINGS=0

log_check() {
    echo -e "${BLUE}[CHECK]${NC} $1"
}

log_ok() {
    echo -e "${GREEN}  ✓${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}  ⚠${NC} $1"
    WARNINGS=$((WARNINGS + 1))
}

log_error() {
    echo -e "${RED}  ✗${NC} $1"
    ERRORS=$((ERRORS + 1))
}

echo "============================================================================="
echo "  JobScout Configuration Validation"
echo "============================================================================="
echo ""

# Check Python version
log_check "Checking Python version..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
    MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
    MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)
    if [ "$MAJOR" -ge 3 ] && [ "$MINOR" -ge 11 ]; then
        log_ok "Python $PYTHON_VERSION (requires 3.11+)"
    else
        log_error "Python $PYTHON_VERSION found, but 3.11+ required"
    fi
else
    log_error "Python 3 not found. Install Python 3.11+"
fi

# Check uv
log_check "Checking uv package manager..."
if command -v uv &> /dev/null; then
    UV_VERSION=$(uv --version 2>&1 | awk '{print $2}')
    log_ok "uv $UV_VERSION installed"
else
    log_error "uv not found. Install with: pip install uv"
fi

# Check Docker
log_check "Checking Docker..."
if command -v docker &> /dev/null; then
    if docker info &> /dev/null; then
        log_ok "Docker is running"
    else
        log_error "Docker installed but not running. Start with: docker desktop"
    fi
else
    log_error "Docker not installed"
fi

# Check Node.js
log_check "Checking Node.js..."
if command -v node &> /dev/null; then
    NODE_VERSION=$(node --version 2>&1 | sed 's/v//')
    MAJOR=$(echo $NODE_VERSION | cut -d. -f1)
    if [ "$MAJOR" -ge 18 ]; then
        log_ok "Node.js $NODE_VERSION (requires 18+)"
    else
        log_warn "Node.js $NODE_VERSION found, but 18+ recommended"
    fi
else
    log_error "Node.js not found. Install from https://nodejs.org"
fi

# Check resume.json
log_check "Checking resume.json..."
if [ -f "resume.json" ]; then
    if python3 -m json.tool resume.json > /dev/null 2>&1; then
        log_ok "resume.json exists and is valid JSON"
    else
        log_error "resume.json is invalid JSON. Fix syntax errors"
    fi
else
    log_warn "resume.json not found. Create with: cp resume.example.json resume.json"
fi

# Check config.yaml
log_check "Checking config.yaml..."
if [ -f "config.yaml" ]; then
    # Try to parse with PyYAML (use uv run python since system python may not have PyYAML)
    YAML_ERROR=$(uv run python -c "import yaml; yaml.safe_load(open('config.yaml'))" 2>&1)
    if [ $? -eq 0 ]; then
        log_ok "config.yaml exists and is valid YAML"
    else
        log_error "config.yaml is invalid YAML:"
        echo "       $YAML_ERROR" | head -3
    fi
else
    log_error "config.yaml not found"
fi

# Check .env
log_check "Checking .env file..."
if [ -f ".env" ]; then
    log_ok ".env file exists"
    
    # Check for Discord webhook if notifications are enabled
    if grep -q "DISCORD_WEBHOOK_URL=" .env; then
        if grep -q "DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/" .env; then
            log_ok "Discord webhook configured"
        else
            log_warn "Discord webhook URL may be invalid or missing"
        fi
    else
        log_warn "No Discord webhook found in .env (notifications may not work)"
    fi
else
    log_warn ".env not found. Create with: cp .env.example .env"
fi

# Check ports availability
log_check "Checking port availability..."
PORTS=(5432 6379 8080 5173)
PORT_NAMES=("PostgreSQL" "Redis" "Backend" "Frontend")

for i in "${!PORTS[@]}"; do
    PORT=${PORTS[$i]}
    NAME=${PORT_NAMES[$i]}
    if lsof -ti:$PORT > /dev/null 2>&1; then
        PROCESS=$(lsof -ti:$PORT | head -1)
        log_warn "Port $PORT ($NAME) already in use by PID $PROCESS"
        echo "       Kill with: kill $PROCESS"
    else
        log_ok "Port $PORT ($NAME) available"
    fi
done

# Check Ollama (optional)
log_check "Checking Ollama..."
if curl -s http://localhost:11434/api/tags > /dev/null 2>&1; then
    log_ok "Ollama is running on localhost:11434"
    
    # Check for required models
    MODELS=$(curl -s http://localhost:11434/api/tags | python3 -c "import sys, json; print('\n'.join([m['name'] for m in json.load(sys.stdin)['models']]))" 2>/dev/null || echo "")
    
    if echo "$MODELS" | grep -q "qwen3:14b"; then
        log_ok "Model qwen3:14b found"
    else
        log_warn "Model qwen3:14b not found. Pull with: ollama pull qwen3:14b"
    fi
    
    if echo "$MODELS" | grep -q "qwen3-embedding:4b"; then
        log_ok "Model qwen3-embedding:4b found"
    else
        log_warn "Model qwen3-embedding:4b not found. Pull with: ollama pull qwen3-embedding:4b"
    fi
else
    log_warn "Ollama not running. Start with: ollama serve"
    echo "       Or use Docker: docker-compose --profile docker-ollama up -d"
fi

# Summary
echo ""
echo "============================================================================="
if [ $ERRORS -eq 0 ] && [ $WARNINGS -eq 0 ]; then
    echo -e "${GREEN}✓ All checks passed!${NC} You're ready to run JobScout."
    echo ""
    echo "  Start with: ./scripts/setup_local_env/start.sh --docker --backend --frontend"
elif [ $ERRORS -eq 0 ]; then
    echo -e "${YELLOW}⚠ $WARNINGS warning(s) found.${NC} JobScout may run, but some features might not work."
else
    echo -e "${RED}✗ $ERRORS error(s) and $WARNINGS warning(s) found.${NC} Fix errors before running."
fi
echo "============================================================================="

exit $ERRORS
