#!/bin/bash
# =============================================================================
# JobScout - Spin Down Script
# =============================================================================
# Usage:
#   ./stop.sh                    Stop all services
#   ./stop.sh --docker           Stop Docker services only
#   ./stop.sh --backend          Stop backend only
#   ./stop.sh --frontend         Stop frontend only
#   ./stop.sh --all              Stop everything (same as default)
#
# Options:
#   -d, --docker    Stop Docker services (postgres, redis, ollama)
#   -p, --postgres  Stop PostgreSQL only
#   -r, --redis    Stop Redis only
#   -b, --backend   Stop FastAPI backend server
#   -f, --frontend  Stop Vite frontend dev server
#   -a, --all       Stop all services (default)
#   -h, --help      Show this help message
#
# Examples:
#   ./stop.sh                    Stop everything
#   ./stop.sh --docker          Stop Docker services only
#   ./stop.sh --backend         Stop backend only
# =============================================================================

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DOCKER_COMPOSE_FILE="${PROJECT_ROOT}/docker-compose.yml"
BACKEND_PORT=8080
FRONTEND_PORT=5173

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Print help message
show_help() {
    head -25 "$0" | tail -23
}

# Parse command line arguments
parse_args() {
    STOP_DOCKER=false
    STOP_BACKEND=false
    STOP_FRONTEND=false
    STOP_POSTGRES=false
    STOP_REDIS=false

    while [[ $# -gt 0 ]]; do
        case $1 in
            -d|--docker)
                STOP_DOCKER=true
                shift
                ;;
            -p|--postgres)
                STOP_POSTGRES=true
                shift
                ;;
            -r|--redis)
                STOP_REDIS=true
                shift
                ;;
            -b|--backend)
                STOP_BACKEND=true
                shift
                ;;
            -f|--frontend)
                STOP_FRONTEND=true
                shift
                ;;
            -a|--all)
                STOP_DOCKER=true
                STOP_BACKEND=true
                STOP_FRONTEND=true
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done

    # Default: stop all if nothing specified
    if [ "$STOP_DOCKER" = false ] && [ "$STOP_POSTGRES" = false ] && [ "$STOP_REDIS" = false ] && [ "$STOP_BACKEND" = false ] && [ "$STOP_FRONTEND" = false ]; then
        STOP_DOCKER=true
        STOP_BACKEND=true
        STOP_FRONTEND=true
    fi
}

# Stop Docker services
stop_docker() {
    log_info "Stopping Docker services..."

    # Check if docker-compose.yml exists
    if [ ! -f "${DOCKER_COMPOSE_FILE}" ]; then
        log_warn "docker-compose.yml not found at ${DOCKER_COMPOSE_FILE}, skipping Docker services"
        return
    fi

    # Determine which services to stop
    SERVICES_TO_STOP=""
    
    if [ "$STOP_POSTGRES" = true ] || [ "$STOP_REDIS" = true ]; then
        if [ "$STOP_POSTGRES" = true ]; then
            SERVICES_TO_STOP="${SERVICES_TO_STOP} postgres"
            log_info "Stopping PostgreSQL..."
        fi
        if [ "$STOP_REDIS" = true ]; then
            SERVICES_TO_STOP="${SERVICES_TO_STOP} redis"
            log_info "Stopping Redis..."
        fi
    fi

    # Check if any containers are running
    if docker-compose -f "${DOCKER_COMPOSE_FILE}" ps -q 2>/dev/null | grep -q .; then
        if [ -n "$SERVICES_TO_STOP" ]; then
            docker-compose -f "${DOCKER_COMPOSE_FILE}" stop ${SERVICES_TO_STOP} 2>/dev/null || true
        else
            docker-compose -f "${DOCKER_COMPOSE_FILE}" down --remove-orphans 2>/dev/null || true
        fi
        log_success "Docker services stopped"
    else
        log_info "No Docker services running"
    fi
}

# Kill only child processes by port (don't kill parent like npm)
# This is used for frontend to avoid killing npm which can affect browser
kill_child_only_by_port() {
    local port=$1
    
    # Get PIDs listening on port
    local pids
    pids=$(lsof -ti:${port} 2>/dev/null)
    
    if [ -z "$pids" ]; then
        return 1
    fi
    
    # Only kill child processes (node, vite), not parent (npm)
    for pid in $pids; do
        local process_name
        process_name=$(ps -o comm= -p "$pid" 2>/dev/null | tr -d ' ')
        # Kill node/vite processes but not npm
        if [ "$process_name" = "node" ] || [ "$process_name" = "vite" ]; then
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
    
    sleep 1
    
    # Kill any remaining processes on port
    lsof -ti:${port} 2>/dev/null | xargs kill -9 2>/dev/null || true
    
    return 0
}

# Kill a process tree by port (kills parent wrapper processes like uv and their children)
# This is more aggressive - use for backend only
kill_process_tree_by_port() {
    local port=$1
    local pids_file
    pids_file=$(mktemp)
    
    # Get PIDs listening on port
    lsof -ti:${port} 2>/dev/null > "$pids_file"
    
    if [ ! -s "$pids_file" ]; then
        rm -f "$pids_file"
        return 1
    fi
    
    # Collect all parent PIDs to avoid duplicates
    declare -a parents=()
    while read -r pid; do
        parent=$(ps -o ppid= -p "$pid" 2>/dev/null | tr -d ' ')
        if [ -n "$parent" ] && [ "$parent" -ne 1 ] && [ "$parent" -ne $$ ]; then
            # Check if parent is already in list
            local found=false
            for existing in "${parents[@]}"; do
                if [ "$existing" = "$parent" ]; then
                    found=true
                    break
                fi
            done
            if [ "$found" = false ]; then
                parents+=("$parent")
            fi
        fi
    done < "$pids_file"
    
    # Kill parents first (top-down) with SIGKILL for reliability
    for parent in "${parents[@]}"; do
        kill -9 "$parent" 2>/dev/null || true
    done
    sleep 1
    
    # Kill all child processes with SIGKILL for reliability
    while read -r pid; do
        kill -9 "$pid" 2>/dev/null || true
    done < "$pids_file"
    
    rm -f "$pids_file"
    return 0
}

# Stop Backend
stop_backend() {
    log_info "Stopping backend on port ${BACKEND_PORT}..."

    # Kill by port - kill entire process tree to catch parent (uv) and child (uvicorn)
    if lsof -ti:${BACKEND_PORT} >/dev/null 2>&1; then
        if kill_process_tree_by_port ${BACKEND_PORT}; then
            log_success "Backend stopped"
        else
            log_error "Failed to stop backend"
        fi
    else
        # Also try by process name as fallback
        if pgrep -f "uvicorn.*web.backend.app" > /dev/null; then
            pkill -f "uvicorn.*web.backend.app" 2>/dev/null || true
            # Also kill uv run wrapper if it exists
            pgrep -f "uv run.*uvicorn" > /dev/null && pkill -f "uv run.*uvicorn" 2>/dev/null || true
            log_success "Backend stopped (by process name)"
        else
            log_info "Backend not running on port ${BACKEND_PORT}"
        fi
    fi
}

# Stop Frontend
stop_frontend() {
    log_info "Stopping frontend on port ${FRONTEND_PORT}..."

    # Kill only child processes (node/vite) without killing npm parent
    if lsof -ti:${FRONTEND_PORT} >/dev/null 2>&1; then
        if kill_child_only_by_port ${FRONTEND_PORT}; then
            log_success "Frontend stopped"
        else
            log_error "Failed to stop frontend"
        fi
    else
        # Also try by process name as fallback
        if pgrep -f "vite" > /dev/null; then
            pkill -f "vite" 2>/dev/null || true
            log_success "Frontend stopped (by process name)"
        else
            log_info "Frontend not running on port ${FRONTEND_PORT}"
        fi
    fi
}

# Print status summary
print_summary() {
    echo ""
    echo "============================================================================="
    echo "  JobScout Services Stopped"
    echo "============================================================================="
    echo ""
}

# Main function
main() {
    parse_args "$@"

    echo "============================================================================="
    echo "  JobScout Spin Down Script"
    echo "============================================================================="
    echo ""

    if [ "$STOP_DOCKER" = true ] || [ "$STOP_POSTGRES" = true ] || [ "$STOP_REDIS" = true ]; then
        stop_docker
        echo ""
    fi

    if [ "$STOP_BACKEND" = true ]; then
        stop_backend
        echo ""
    fi

    if [ "$STOP_FRONTEND" = true ]; then
        stop_frontend
        echo ""
    fi

    print_summary
}

main "$@"
