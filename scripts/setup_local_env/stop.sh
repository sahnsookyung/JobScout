#!/bin/bash
# =============================================================================
# JobScout - Spin Down Script
# =============================================================================
# Usage:
#   ./stop.sh                    Stop all services (default)
#   ./stop.sh --infra            Stop Docker infrastructure only
#   ./stop.sh --web-app          Stop web application only
#   ./stop.sh --web-ui           Stop web UI only
#   ./stop.sh --microservices    Stop pipeline microservices
#
# Options:
#   -i, --infra         Stop Docker infrastructure (PostgreSQL, Redis)
#   -d, --docker        Stop Docker infrastructure (backward compat for -i)
#   -p, --database      Stop PostgreSQL only
#   -r, --redis         Stop Redis only
#   -a, --web-app       Stop FastAPI web application server
#   -u, --web-ui        Stop Vite frontend UI dev server
#   -m, --microservices Stop pipeline microservices
#   -A, --all           Stop all services (same as default)
#   -h, --help          Show this help message
#
# Default behavior (no options):
#   Stops: all running services (infra + web-app + web-ui + microservices)
#
# Examples:
#   ./stop.sh                    Stop everything (default)
#   ./stop.sh --infra           Stop only Docker infrastructure
#   ./stop.sh --web-app         Stop only web application
#   ./stop.sh --web-ui          Stop only web UI
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

# Build compose args from available compose files
build_compose_args() {
    COMPOSE_ARGS=(-f "${DOCKER_COMPOSE_FILE}")
    if [[ -f "${PROJECT_ROOT}/docker-compose.pipeline.yml" ]]; then
        COMPOSE_ARGS+=(-f "${PROJECT_ROOT}/docker-compose.pipeline.yml")
    fi
    if [[ -f "${PROJECT_ROOT}/docker-compose.web.yml" ]]; then
        COMPOSE_ARGS+=(-f "${PROJECT_ROOT}/docker-compose.web.yml")
    fi
    return 0
}

# Print help message
show_help() {
    head -25 "$0" | tail -23
}

# Parse command line arguments
parse_args() {
    STOP_INFRA=false
    STOP_WEB_APP=false
    STOP_WEB_UI=false
    STOP_MICROSERVICES=false
    STOP_DATABASE=false
    STOP_REDIS=false
    STOP_ALL=false

    while [[ $# -gt 0 ]]; do
        case $1 in
            # Stop everything
            -A|--all)
                STOP_ALL=true
                shift
                ;;
            # Infrastructure (new names)
            -i|--infra)
                STOP_INFRA=true
                shift
                ;;
            # Infrastructure (backward compatibility)
            -d|--docker)
                STOP_INFRA=true
                shift
                ;;
            # Database
            -p|--database|--postgres)
                STOP_DATABASE=true
                shift
                ;;
            # Redis
            -r|--redis)
                STOP_REDIS=true
                shift
                ;;
            # Web application (new names)
            -a|--web-app)
                STOP_WEB_APP=true
                shift
                ;;
            # Web application (backward compatibility)
            -b|--backend)
                STOP_WEB_APP=true
                shift
                ;;
            # Web UI (new names)
            -u|--web-ui)
                STOP_WEB_UI=true
                shift
                ;;
            # Web UI (backward compatibility)
            -f|--frontend)
                STOP_WEB_UI=true
                shift
                ;;
            # Microservices
            -m|--microservices)
                STOP_MICROSERVICES=true
                shift
                ;;
            # Help
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
    if [[ "$STOP_INFRA" == false ]] && [[ "$STOP_DATABASE" == false ]] && [[ "$STOP_REDIS" == false ]] && \
       [[ "$STOP_WEB_APP" == false ]] && [[ "$STOP_WEB_UI" == false ]] && \
       [[ "$STOP_MICROSERVICES" == false ]] && [[ "$STOP_ALL" == false ]]; then
        STOP_ALL=true
    fi
    
    # --all flag enables everything
    if [[ "$STOP_ALL" == true ]]; then
        STOP_INFRA=true
        STOP_WEB_APP=true
        STOP_WEB_UI=true
        STOP_MICROSERVICES=true
    fi
}

# Stop Docker services
stop_docker() {
    log_info "Stopping Docker infrastructure..."

    # Check if docker-compose.yml exists
    if [ ! -f "${DOCKER_COMPOSE_FILE}" ]; then
        log_warn "docker-compose.yml not found at ${DOCKER_COMPOSE_FILE}, skipping Docker services"
        return
    fi

    # Build Compose args
    build_compose_args

    # Determine which services to stop
    SERVICES_TO_STOP=""

    if [[ "$STOP_DATABASE" == true ]] || [[ "$STOP_REDIS" == true ]]; then
        if [[ "$STOP_DATABASE" == true ]]; then
            SERVICES_TO_STOP="${SERVICES_TO_STOP} postgres"
            log_info "Stopping PostgreSQL..."
        fi
        if [[ "$STOP_REDIS" == true ]]; then
            SERVICES_TO_STOP="${SERVICES_TO_STOP} redis"
            log_info "Stopping Redis..."
        fi
    fi

    # Check if any containers are running
    if docker compose "${COMPOSE_ARGS[@]}" ps -q 2>/dev/null | grep -q .; then
        if [ -n "$SERVICES_TO_STOP" ]; then
            docker compose "${COMPOSE_ARGS[@]}" stop ${SERVICES_TO_STOP} 2>/dev/null || true
        else
            docker compose "${COMPOSE_ARGS[@]}" down --remove-orphans 2>/dev/null || true
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

# Stop Web Application
stop_web_app() {
    log_info "Stopping web application on port ${BACKEND_PORT}..."

    # Kill by port - kill entire process tree to catch parent (uv) and child (uvicorn)
    if lsof -ti:${BACKEND_PORT} >/dev/null 2>&1; then
        if kill_process_tree_by_port ${BACKEND_PORT}; then
            log_success "Web application stopped"
        else
            log_error "Failed to stop web application"
        fi
    else
        # Also try by process name as fallback
        if pgrep -f "uvicorn.*web.backend.app" > /dev/null; then
            pkill -f "uvicorn.*web.backend.app" 2>/dev/null || true
            # Also kill uv run wrapper if it exists
            pgrep -f "uv run.*uvicorn" > /dev/null && pkill -f "uv run.*uvicorn" 2>/dev/null || true
            log_success "Web application stopped (by process name)"
        else
            log_info "Web application not running on port ${BACKEND_PORT}"
        fi
    fi
    return 0
}

# Stop Web UI
stop_web_ui() {
    log_info "Stopping web UI on port ${FRONTEND_PORT}..."

    # Kill only child processes (node/vite) without killing npm parent
    if lsof -ti:${FRONTEND_PORT} >/dev/null 2>&1; then
        if kill_child_only_by_port ${FRONTEND_PORT}; then
            log_success "Web UI stopped"
        else
            log_error "Failed to stop web UI"
        fi
    else
        # Also try by process name as fallback
        if pgrep -f "vite" > /dev/null; then
            pkill -f "vite" 2>/dev/null || true
            log_success "Web UI stopped (by process name)"
        else
            log_info "Web UI not running on port ${FRONTEND_PORT}"
        fi
    fi
    return 0
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

    if [[ "$STOP_INFRA" == true ]] || [[ "$STOP_DATABASE" == true ]] || [[ "$STOP_REDIS" == true ]]; then
        stop_docker
        echo ""
    fi

    if [[ "$STOP_WEB_APP" == true ]]; then
        stop_web_app
        echo ""
    fi

    if [[ "$STOP_WEB_UI" == true ]]; then
        stop_web_ui
        echo ""
    fi

    if [[ "$STOP_MICROSERVICES" == true ]]; then
        log_info "Stopping microservices..."
        if [[ ! -f "${DOCKER_COMPOSE_FILE}" ]]; then
            log_warn "docker-compose.yml not found, skipping microservices"
        elif [[ ! -f "${PROJECT_ROOT}/docker-compose.pipeline.yml" ]]; then
            log_warn "docker-compose.pipeline.yml not found, skipping microservices"
        else
            # Stop pipeline services via docker compose
            build_compose_args
            docker compose "${COMPOSE_ARGS[@]}" stop extraction embeddings scorer-matcher orchestrator 2>/dev/null || true
            log_success "Microservices stopped"
        fi
        echo ""
    fi

    print_summary
}

main "$@"
