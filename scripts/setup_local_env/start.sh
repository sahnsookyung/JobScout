#!/bin/bash
# =============================================================================
# JobScout - Full Stack Startup Script
# =============================================================================
# Usage:
#   ./start.sh --docker --backend --frontend    Start everything
#   ./start.sh --backend                        Backend only
#   ./start.sh --frontend                       Frontend only
#   ./start.sh --docker --backend --frontend --block  Block and show all logs
#   ./logs.sh -f                                Tail all logs in real-time
#
# Options:
#   -d, --docker    Start Docker services (postgres, redis)
#   -p, --postgres  Start PostgreSQL only (within Docker)
#   -r, --redis    Start Redis only (within Docker)
#   -b, --backend   Start FastAPI backend server
#   -f, --frontend  Start Vite frontend dev server
#   -o, --ollama    Include Ollama (local embeddings)
#   -c, --clean     Stop existing services first
#   -h, --help      Show this help message
#
# Examples:
#   ./start.sh --docker --backend --frontend    Full stack
#   ./start.sh --backend --frontend --block     Full stack, block and show logs
#   ./start.sh --backend                        Backend only
# =============================================================================

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"  # Go up 2 levels to project root
LOGS_DIR="${SCRIPT_DIR}/logs"  # Logs stay in scripts/setup_local_env/logs
BACKEND_PORT=8080
FRONTEND_PORT=5173
DOCKER_COMPOSE_FILE="${PROJECT_ROOT}/docker-compose.yml"
DOCKER_COMPOSE_PROFILE=""

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

# Ensure logs directory exists
ensure_logs_dir() {
    if [ ! -d "${LOGS_DIR}" ]; then
        mkdir -p "${LOGS_DIR}"
        log_info "Created logs directory: ${LOGS_DIR}"
    fi
}

# Print help message
show_help() {
    head -32 "$0" | tail -28
}

# Parse command line arguments
parse_args() {
    DOCKER=false
    BACKEND=false
    FRONTEND=false
    OLLAMA=false
    CLEAN=false
    BLOCK=false
    POSTGRES=false
    REDIS=false

    while [[ $# -gt 0 ]]; do
        case $1 in
            -d|--docker)
                DOCKER=true
                shift
                ;;
            -p|--postgres)
                POSTGRES=true
                shift
                ;;
            -r|--redis)
                REDIS=true
                shift
                ;;
            -b|--backend)
                BACKEND=true
                shift
                ;;
            -f|--frontend)
                FRONTEND=true
                shift
                ;;
            -o|--ollama)
                OLLAMA=true
                shift
                ;;
            -c|--clean)
                CLEAN=true
                shift
                ;;
            --block)
                BLOCK=true
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

    # Default: enable all if nothing specified
    if [ "$DOCKER" = false ] && [ "$POSTGRES" = false ] && [ "$REDIS" = false ] && [ "$BACKEND" = false ] && [ "$FRONTEND" = false ]; then
        DOCKER=true
        BACKEND=true
        FRONTEND=true
    fi
}

# Stop existing services
stop_services() {
    log_info "Stopping existing services..."

    # Stop Docker services
    if docker-compose -f "${DOCKER_COMPOSE_FILE}" ps -q 2>/dev/null | grep -q .; then
        log_info "Stopping Docker services..."
        docker-compose -f "${DOCKER_COMPOSE_FILE}" down --remove-orphans 2>/dev/null || true
    fi

    # Kill backend process
    if lsof -ti:${BACKEND_PORT} >/dev/null 2>&1; then
        log_info "Stopping backend on port ${BACKEND_PORT}..."
        kill $(lsof -ti:${BACKEND_PORT}) 2>/dev/null || true
    fi

    # Kill frontend process (Vite uses 5173)
    if lsof -ti:${FRONTEND_PORT} >/dev/null 2>&1; then
        log_info "Stopping frontend on port ${FRONTEND_PORT}..."
        kill $(lsof -ti:${FRONTEND_PORT}) 2>/dev/null || true
    fi

    log_success "Existing services stopped"
}

# Start Docker services
start_docker() {
    log_info "Starting Docker services..."
    
    # Determine which services to start
    SERVICES_TO_START=""
    
    if [ "$POSTGRES" = true ] || [ "$REDIS" = true ]; then
        # Selective start - only start specific services
        if [ "$POSTGRES" = true ]; then
            SERVICES_TO_START="${SERVICES_TO_START} postgres"
            log_info "Starting PostgreSQL only..."
        fi
        if [ "$REDIS" = true ]; then
            SERVICES_TO_START="${SERVICES_TO_START} redis"
            log_info "Starting Redis only..."
        fi
    else
        # Start all default services
        log_info "Starting all Docker services (postgres, redis)..."
    fi

    # Set compose file
    COMPOSE_FILE="${DOCKER_COMPOSE_FILE}"

    # Set profile for Ollama if requested
    if [ "$OLLAMA" = true ]; then
        DOCKER_COMPOSE_PROFILE="--profile docker-ollama"
        log_info "Ollama profile enabled"
    fi

    # Check if docker-compose.yml exists
    if [ ! -f "${COMPOSE_FILE}" ]; then
        log_error "docker-compose.yml not found at ${COMPOSE_FILE}"
        exit 1
    fi

    # Start services
    if [ -n "$SERVICES_TO_START" ]; then
        docker-compose -f "${COMPOSE_FILE}" up -d ${DOCKER_COMPOSE_PROFILE} ${SERVICES_TO_START}
    else
        docker-compose -f "${COMPOSE_FILE}" up -d ${DOCKER_COMPOSE_PROFILE}
    fi

    # Wait for PostgreSQL if it was started
    if [ "$POSTGRES" = true ] || [ "$DOCKER" = true ] && [ "$POSTGRES" = false ] && [ "$REDIS" = false ]; then
        log_info "Waiting for PostgreSQL..."
        timeout 30 bash -c 'until docker-compose -f '"${COMPOSE_FILE}"' exec -T postgres pg_isready -U user -d jobscout; do sleep 1; done' 2>/dev/null || {
            log_warn "PostgreSQL may not be ready yet, continuing..."
        }
    fi

    log_success "Docker services started"
    log_info "  - PostgreSQL: localhost:5432"
    log_info "  - Redis: localhost:6379"
    if [ "$OLLAMA" = true ]; then
        log_info "  - Ollama: localhost:11434"
    fi

    # Start background log capture for Docker services
    ensure_logs_dir
    
    # Capture postgres logs
    if docker-compose -f "${COMPOSE_FILE}" ps postgres 2>/dev/null | grep -q "Up"; then
        docker-compose -f "${COMPOSE_FILE}" logs -f postgres > "${LOGS_DIR}/postgres.log" 2>&1 &
        log_info "Capturing PostgreSQL logs to ${LOGS_DIR}/postgres.log"
    fi

    # Capture main-driver logs (if running)
    if docker-compose -f "${COMPOSE_FILE}" ps main-driver 2>/dev/null | grep -q "Up"; then
        docker-compose -f "${COMPOSE_FILE}" logs -f main-driver > "${LOGS_DIR}/main-driver.log" 2>&1 &
        log_info "Capturing main-driver logs to ${LOGS_DIR}/main-driver.log"
    fi
}

# Start Backend
start_backend() {
    log_info "Starting FastAPI backend..."

    # Check if uv is available
    if ! command -v uv &> /dev/null; then
        log_error "uv is not installed. Install with: pip install uv"
        exit 1
    fi

    # Check if port is already in use
    if lsof -ti:${BACKEND_PORT} >/dev/null 2>&1; then
        log_warn "Port ${BACKEND_PORT} is already in use. Attempting to kill..."
        kill $(lsof -ti:${BACKEND_PORT}) 2>/dev/null || true
        sleep 2
    fi

    # Start backend
    cd "${PROJECT_ROOT}"
    uv run python -m uvicorn web.backend.app:app --host 0.0.0.0 --reload --port ${BACKEND_PORT} > "${LOGS_DIR}/backend.log" 2>&1 &

    BACKEND_PID=$!
    log_info "Backend started with PID: ${BACKEND_PID}"
    log_info "  - Dashboard: http://localhost:${BACKEND_PORT}"
    log_info "  - API Docs: http://localhost:${BACKEND_PORT}/docs"

    # Wait for backend to be ready
    log_info "Waiting for backend to be ready..."
    for i in {1..30}; do
        if curl -s "http://localhost:${BACKEND_PORT}/health" >/dev/null 2>&1; then
            log_success "Backend is ready!"
            return 0
        fi
        sleep 1
    done

    log_error "Backend failed to start. Check logs at: ${LOGS_DIR}/backend.log"
    return 1
}

# Start Frontend
start_frontend() {
    log_info "Starting Vite frontend..."

    # Check if package.json exists
    if [ ! -f "${PROJECT_ROOT}/web/frontend/package.json" ]; then
        log_error "Frontend package.json not found"
        return 1
    fi

    # Check if port is already in use
    if lsof -ti:${FRONTEND_PORT} >/dev/null 2>&1; then
        log_warn "Port ${FRONTEND_PORT} is already in use. Attempting to kill..."
        kill $(lsof -ti:${FRONTEND_PORT}) 2>/dev/null || true
        sleep 2
    fi

    # Start frontend
    cd "${PROJECT_ROOT}/web/frontend"
    npm run dev > "${LOGS_DIR}/frontend.log" 2>&1 &

    FRONTEND_PID=$!
    log_info "Frontend started with PID: ${FRONTEND_PID}"
    log_info "  - Frontend: http://localhost:${FRONTEND_PORT}"
    log_info "  - API Proxy: http://localhost:${FRONTEND_PORT}/api -> localhost:${BACKEND_PORT}"

    # Wait for frontend to be ready
    log_info "Waiting for frontend to be ready..."
    for i in {1..30}; do
        if curl -s "http://localhost:${FRONTEND_PORT}" >/dev/null 2>&1; then
            log_success "Frontend is ready!"
            return 0
        fi
        sleep 1
    done

    log_error "Frontend failed to start. Check logs at: ${LOGS_DIR}/frontend.log"
    return 1
}

# Print status summary
print_summary() {
    echo ""
    echo "============================================================================="
    echo "  JobScout is running!"
    echo "============================================================================="
    echo ""
    if [ "$FRONTEND" = true ]; then
        echo -e "  ${GREEN}Frontend${NC}:  http://localhost:${FRONTEND_PORT}"
    fi
    if [ "$BACKEND" = true ]; then
        echo -e "  ${GREEN}Backend${NC}:   http://localhost:${BACKEND_PORT}"
        echo -e "  ${GREEN}API Docs${NC}:  http://localhost:${BACKEND_PORT}/docs"
    fi
    echo ""
    echo "  Logs:"
    echo -e "    ${BLUE}Backend${NC}:     ${LOGS_DIR}/backend.log"
    echo -e "    ${BLUE}Frontend${NC}:    ${LOGS_DIR}/frontend.log"
    if [ "$DOCKER" = true ] || [ "$POSTGRES" = true ]; then
        echo -e "    ${BLUE}PostgreSQL${NC}:  ${LOGS_DIR}/postgres.log"
    fi
    if [ "$DOCKER" = true ]; then
        echo -e "    ${BLUE}Main Driver${NC}: ${LOGS_DIR}/main-driver.log"
    fi
    echo ""
    echo "  To view logs in real-time:"
    echo -e "    ${YELLOW}tail -f ${LOGS_DIR}/backend.log${NC}"
    echo -e "    ${YELLOW}tail -f ${LOGS_DIR}/frontend.log${NC}"
    if [ "$DOCKER" = true ] || [ "$POSTGRES" = true ]; then
        echo -e "    ${YELLOW}tail -f ${LOGS_DIR}/postgres.log${NC}"
    fi
    if [ "$DOCKER" = true ]; then
        echo -e "    ${YELLOW}tail -f ${LOGS_DIR}/main-driver.log${NC}"
    fi
    echo ""
    echo "  Or use the logs script:"
    echo -e "    ${YELLOW}./scripts/setup_local_env/logs.sh -f${NC}   (follow all logs)"
    echo "    ./scripts/setup_local_env/logs.sh backend    (backend only)"
    echo "    ./scripts/setup_local_env/logs.sh frontend   (frontend only)"
    echo ""
    echo "  To stop:"
    echo -e "    ${YELLOW}pkill -f 'uvicorn'${NC}   (backend)"
    echo -e "    ${YELLOW}pkill -f 'vite'${NC}       (frontend)"
    echo "    docker-compose down  (docker services)"
    echo ""
}

# Main function
main() {
    ensure_logs_dir
    parse_args "$@"

    echo "============================================================================="
    echo "  JobScout Startup Script"
    echo "============================================================================="
    echo ""

    if [ "$CLEAN" = true ]; then
        stop_services
        echo ""
    fi

    if [ "$DOCKER" = true ] || [ "$POSTGRES" = true ] || [ "$REDIS" = true ]; then
        start_docker
        echo ""
    fi

    if [ "$BACKEND" = true ]; then
        start_backend
        echo ""
    fi

    if [ "$FRONTEND" = true ]; then
        start_frontend
        echo ""
    fi

    print_summary

    if [ "$BLOCK" = true ]; then
        log_info "Blocking and showing logs (Ctrl+C to stop)..."
        echo ""
        echo "--- Backend Log ---"
        tail -f "${LOGS_DIR}/backend.log" &
        TAIL_PID=$!
        echo "--- Frontend Log ---"
        tail -f "${LOGS_DIR}/frontend.log" &
        wait $TAIL_PID
    fi
}

main "$@"
