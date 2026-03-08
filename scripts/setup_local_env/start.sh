#!/bin/bash
# =============================================================================
# JobScout - Full Stack Startup Script
# =============================================================================
# Usage:
#   ./start.sh                              Start full stack (default)
#   ./start.sh --infra --web-app --web-ui   Start specific services
#   ./start.sh --web-app                    Web API only
#   ./start.sh --web-ui                     Frontend UI only
#   ./start.sh --microservices              Pipeline microservices only
#   ./logs.sh -f                            Tail all logs in real-time
#
# Options:
#   -i, --infra         Start infrastructure (PostgreSQL, Redis) via Docker
#   -d, --database      Start PostgreSQL only (within Docker)
#   -r, --redis         Start Redis only (within Docker)
#   -a, --web-app       Start FastAPI web application server (port 8080)
#   -u, --web-ui        Start Vite frontend UI dev server (port 5173)
#   -m, --microservices Start pipeline microservices (extraction, embeddings, scorer-matcher, orchestrator)
#   -o, --ollama        Include Ollama service for local AI embeddings
#   -c, --clean         Stop existing services first
#   -h, --help          Show this help message
#
# Default behavior (no options):
#   Starts: infra + web-app + web-ui + microservices (full stack)
#   This ensures the frontend has data to display and all services are available
#
# Examples:
#   ./start.sh                           Full stack (everything)
#   ./start.sh --web-app --web-ui        API + UI only (use existing DB + microservices)
#   ./start.sh --infra --microservices   DB + microservices (no web UI)
#   ./start.sh --clean --web-app         Clean start with web app only
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
    # New descriptive names
    INFRA=false
    WEB_APP=false
    WEB_UI=false
    MICROSERVICES=false
    OLLAMA=false
    CLEAN=false
    BLOCK=false
    DATABASE=false
    REDIS=false
    ALL=false

    while [[ $# -gt 0 ]]; do
        case $1 in
            # Start everything (all services including microservices)
            --all)
                ALL=true
                shift
                ;;
            # Infrastructure (new names)
            -i|--infra)
                INFRA=true
                shift
                ;;
            # Infrastructure (backward compatibility)
            -d|--docker)
                INFRA=true
                shift
                ;;
            # Database
            -p|--database|--postgres)
                DATABASE=true
                shift
                ;;
            # Redis
            -r|--redis)
                REDIS=true
                shift
                ;;
            # Web application (new names)
            -a|--web-app)
                WEB_APP=true
                shift
                ;;
            # Web application (backward compatibility)
            -b|--backend)
                WEB_APP=true
                shift
                ;;
            # Web UI (new names)
            -u|--web-ui)
                WEB_UI=true
                shift
                ;;
            # Web UI (backward compatibility)
            -f|--frontend)
                WEB_UI=true
                shift
                ;;
            # Microservices (new names)
            -m|--microservices)
                MICROSERVICES=true
                shift
                ;;
            # Microservices (backward compatibility)
            --pipeline)
                MICROSERVICES=true
                shift
                ;;
            # Ollama
            -o|--ollama)
                OLLAMA=true
                shift
                ;;
            # Clean
            -c|--clean)
                CLEAN=true
                shift
                ;;
            # Block
            --block)
                BLOCK=true
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

    # Default: start common dev stack (infra + web-app + web-ui + microservices) if nothing specified
    # This ensures the frontend has data to display and all services are available
    if [[ "$INFRA" == false ]] && [[ "$DATABASE" == false ]] && [[ "$REDIS" == false ]] && \
       [[ "$WEB_APP" == false ]] && [[ "$WEB_UI" == false ]] && \
       [[ "$MICROSERVICES" == false ]] && [[ "$ALL" == false ]]; then
        INFRA=true
        WEB_APP=true
        WEB_UI=true
        MICROSERVICES=true
        log_info "No options specified, starting full stack (infra + web-app + web-ui + microservices)"
    fi
    
    # --all flag enables everything
    if [[ "$ALL" == true ]]; then
        INFRA=true
        WEB_APP=true
        WEB_UI=true
        MICROSERVICES=true
    fi
}

# Stop existing services
stop_services() {
    log_info "Stopping existing services..."

    # Build compose args to stop all potential services (using array for paths with spaces)
    local compose_args=(-f "${DOCKER_COMPOSE_FILE}")
    if [[ -f "${PROJECT_ROOT}/docker-compose.pipeline.yml" ]]; then
        compose_args+=(-f "${PROJECT_ROOT}/docker-compose.pipeline.yml")
    fi
    if [[ -f "${PROJECT_ROOT}/docker-compose.web.yml" ]]; then
        compose_args+=(-f "${PROJECT_ROOT}/docker-compose.web.yml")
    fi

    # Stop Docker services
    if docker compose "${compose_args[@]}" ps -q 2>/dev/null | grep -q .; then
        log_info "Stopping Docker services..."
        docker compose "${compose_args[@]}" down --remove-orphans 2>/dev/null || true
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

    if [[ "$DATABASE" == true ]] || [[ "$REDIS" == true ]]; then
        # Selective start - only start specific services
        if [[ "$DATABASE" == true ]]; then
            SERVICES_TO_START="${SERVICES_TO_START} postgres"
            log_info "Starting PostgreSQL only..."
        fi
        if [[ "$REDIS" == true ]]; then
            SERVICES_TO_START="${SERVICES_TO_START} redis"
            log_info "Starting Redis only..."
        fi
    elif [[ "$INFRA" == true ]]; then
        # Start all default infra services
        log_info "Starting all Docker services (postgres, redis)..."
    elif [[ "$MICROSERVICES" == true ]]; then
        if [[ "$INFRA" == false ]] && [[ "$DATABASE" == false ]] && [[ "$REDIS" == false ]]; then
            log_info "Microservices require infrastructure, enabling postgres and redis..."
            INFRA=true
            SERVICES_TO_START="postgres redis extraction embeddings scorer-matcher orchestrator"
        else
            log_info "Starting microservices..."
            SERVICES_TO_START="extraction embeddings scorer-matcher orchestrator"
        fi
    else
        # Start all default infra services
        log_info "Starting all Docker services (postgres, redis)..."
    fi

    # Build compose file array (handles paths with spaces)
    local compose_files=(-f "${DOCKER_COMPOSE_FILE}")

    # Check if docker-compose.yml exists (fail fast)
    if [[ ! -f "${DOCKER_COMPOSE_FILE}" ]]; then
        log_error "docker-compose.yml not found at ${PROJECT_ROOT}"
        exit 1
    fi

    # Add pipeline compose file if microservices requested
    if [[ "$MICROSERVICES" == true ]]; then
        if [[ ! -f "${PROJECT_ROOT}/docker-compose.pipeline.yml" ]]; then
            log_error "docker-compose.pipeline.yml not found at ${PROJECT_ROOT}"
            exit 1
        fi
        compose_files+=(-f "${PROJECT_ROOT}/docker-compose.pipeline.yml")
        log_info "Microservices enabled"
    fi

    # Add web compose file if web app requested
    if [[ "$WEB_APP" == true ]]; then
        if [[ -f "${PROJECT_ROOT}/docker-compose.web.yml" ]]; then
            compose_files+=(-f "${PROJECT_ROOT}/docker-compose.web.yml")
        else
            log_warn "docker-compose.web.yml not found, skipping web compose file"
        fi
    fi

    # Set profile for Ollama if requested
    if [[ "$OLLAMA" == true ]]; then
        DOCKER_COMPOSE_PROFILE="--profile docker-ollama"
        log_info "Ollama profile enabled"
    fi

    # Start services
    if [ -n "$SERVICES_TO_START" ]; then
        docker compose "${compose_files[@]}" up -d ${DOCKER_COMPOSE_PROFILE} ${SERVICES_TO_START}
    else
        docker compose "${compose_files[@]}" up -d ${DOCKER_COMPOSE_PROFILE}
    fi

    # Wait for PostgreSQL if it was started
    if [[ "$DATABASE" == true ]] || [[ "$INFRA" == true ]]; then
        log_info "Waiting for PostgreSQL..."
        timeout 30 bash -c "until docker compose -f '${DOCKER_COMPOSE_FILE}' exec -T postgres pg_isready -U '${POSTGRES_USER:-user}' -d '${POSTGRES_DB:-jobscout}'; do sleep 1; done" 2>/dev/null || {
            log_warn "PostgreSQL may not be ready yet, continuing..."
        }
    fi

    log_success "Docker services started"
    log_info "  - PostgreSQL: localhost:5432"
    log_info "  - Redis: localhost:6379"
    if [[ "$OLLAMA" == true ]]; then
        log_info "  - Ollama: localhost:11434"
    fi

    # Start background log capture for Docker services
    ensure_logs_dir

    # Capture postgres logs
    if docker compose "${compose_files[@]}" ps postgres 2>/dev/null | grep -q "Up"; then
        docker compose "${compose_files[@]}" logs -f postgres > "${LOGS_DIR}/postgres.log" 2>&1 &
        log_info "Capturing PostgreSQL logs to ${LOGS_DIR}/postgres.log"
    fi

    # Capture main-driver logs (if running)
    if docker compose "${compose_files[@]}" ps main-driver 2>/dev/null | grep -q "Up"; then
        docker compose "${compose_files[@]}" logs -f main-driver > "${LOGS_DIR}/main-driver.log" 2>&1 &
        log_info "Capturing main-driver logs to ${LOGS_DIR}/main-driver.log"
    fi

    # Capture microservice logs
    for service in extraction embeddings scorer-matcher orchestrator; do
        if docker compose "${compose_files[@]}" ps $service 2>/dev/null | grep -q "Up"; then
            docker compose "${compose_files[@]}" logs -f $service > "${LOGS_DIR}/${service}.log" 2>&1 &
            log_info "Capturing ${service} logs to ${LOGS_DIR}/${service}.log"
        fi
    done
}

# Start Web Application (FastAPI backend)
start_web_app() {
    log_info "Starting FastAPI web application..."

    # Check if uv is available
    if ! command -v uv &> /dev/null; then
        log_error "uv is not installed. Install with: pip install uv"
        exit 1
    fi

    # Set microservice URLs for local development (when running web app locally but microservices in Docker)
    # These allow the web backend to communicate with microservices on localhost
    export EXTRACTION_URL=${EXTRACTION_URL:-http://localhost:8081}
    export EMBEDDINGS_URL=${EMBEDDINGS_URL:-http://localhost:8082}
    export SCORER_MATCHER_URL=${SCORER_MATCHER_URL:-http://localhost:8083}
    export ORCHESTRATOR_URL=${ORCHESTRATOR_URL:-http://localhost:8084}

    # Check if port is already in use
    if lsof -ti:${BACKEND_PORT} >/dev/null 2>&1; then
        log_warn "Port ${BACKEND_PORT} is already in use. Attempting to kill..."
        kill $(lsof -ti:${BACKEND_PORT}) 2>/dev/null || true
        sleep 2
    fi

    # Start web app
    cd "${PROJECT_ROOT}"
    uv run python -m uvicorn web.backend.app:app --host 0.0.0.0 --reload --port ${BACKEND_PORT} > "${LOGS_DIR}/web-app.log" 2>&1 &

    WEB_APP_PID=$!
    log_info "Web application started with PID: ${WEB_APP_PID}"
    log_info "  - Dashboard: http://localhost:${BACKEND_PORT}"
    log_info "  - API Docs: http://localhost:${BACKEND_PORT}/docs"

    # Wait for web app to be ready
    log_info "Waiting for web application to be ready..."
    for i in {1..30}; do
        if curl -s "http://localhost:${BACKEND_PORT}/health" >/dev/null 2>&1; then
            log_success "Web application is ready!"
            return 0
        fi
        sleep 1
    done

    log_error "Web application failed to start. Check logs at: ${LOGS_DIR}/web-app.log"
    return 1
}

# Start Web UI (Vite frontend)
start_web_ui() {
    log_info "Starting Vite web UI..."

    # Check if package.json exists
    if [ ! -f "${PROJECT_ROOT}/web/frontend/package.json" ]; then
        log_error "Web UI package.json not found"
        return 1
    fi

    # Check if port is already in use
    if lsof -ti:${FRONTEND_PORT} >/dev/null 2>&1; then
        log_warn "Port ${FRONTEND_PORT} is already in use. Attempting to kill..."
        kill $(lsof -ti:${FRONTEND_PORT}) 2>/dev/null || true
        sleep 2
    fi

    # Start web UI
    # Check if npm is available
    if ! command -v npm &> /dev/null; then
        log_error "npm is not installed. Install Node.js and npm first."
        return 1
    fi

    cd "${PROJECT_ROOT}/web/frontend"
    npm run dev > "${LOGS_DIR}/web-ui.log" 2>&1 &

    WEB_UI_PID=$!
    log_info "Web UI started with PID: ${WEB_UI_PID}"
    log_info "  - Web UI: http://localhost:${FRONTEND_PORT}"
    log_info "  - API Proxy: http://localhost:${FRONTEND_PORT}/api -> localhost:${BACKEND_PORT}"

    # Wait for web UI to be ready
    log_info "Waiting for web UI to be ready..."
    for i in {1..30}; do
        if curl -s "http://localhost:${FRONTEND_PORT}" >/dev/null 2>&1; then
            log_success "Web UI is ready!"
            return 0
        fi
        sleep 1
    done

    log_error "Web UI failed to start. Check logs at: ${LOGS_DIR}/web-ui.log"
    return 1
}

# Print status summary
print_summary() {
    echo ""
    echo "============================================================================="
    echo "  JobScout is running!"
    echo "============================================================================="
    echo ""
    if [[ "$WEB_UI" == true ]]; then
        echo -e "  ${GREEN}Web UI${NC}:      http://localhost:${FRONTEND_PORT}"
    fi
    if [[ "$WEB_APP" == true ]]; then
        echo -e "  ${GREEN}Web App${NC}:     http://localhost:${BACKEND_PORT}"
        echo -e "  ${GREEN}API Docs${NC}:    http://localhost:${BACKEND_PORT}/docs"
    fi
    if [[ "$MICROSERVICES" == true ]]; then
        echo -e "  ${GREEN}Microservices:${NC}"
        echo -e "    - Extraction:     http://localhost:8081"
        echo -e "    - Embeddings:     http://localhost:8082"
        echo -e "    - Scorer-Matcher: http://localhost:8083"
        echo -e "    - Orchestrator:   http://localhost:8084"
    fi
    echo ""
    echo "  Logs:"
    if [[ "$WEB_APP" == true ]]; then
        echo -e "    ${BLUE}Web App${NC}:     ${LOGS_DIR}/web-app.log"
    fi
    if [[ "$WEB_UI" == true ]]; then
        echo -e "    ${BLUE}Web UI${NC}:      ${LOGS_DIR}/web-ui.log"
    fi
    if [[ "$INFRA" == true ]] || [[ "$DATABASE" == true ]]; then
        echo -e "    ${BLUE}PostgreSQL${NC}:  ${LOGS_DIR}/postgres.log"
    fi
    if [[ "$INFRA" == true ]]; then
        echo -e "    ${BLUE}Main Driver${NC}: ${LOGS_DIR}/main-driver.log"
    fi
    echo ""
    echo "  To view logs in real-time:"
    if [[ "$WEB_APP" == true ]]; then
        echo -e "    ${YELLOW}tail -f ${LOGS_DIR}/web-app.log${NC}"
    fi
    if [[ "$WEB_UI" == true ]]; then
        echo -e "    ${YELLOW}tail -f ${LOGS_DIR}/web-ui.log${NC}"
    fi
    if [[ "$INFRA" == true ]] || [[ "$DATABASE" == true ]]; then
        echo -e "    ${YELLOW}tail -f ${LOGS_DIR}/postgres.log${NC}"
    fi
    echo ""
    echo "  Or use the logs script:"
    echo -e "    ${YELLOW}./scripts/setup_local_env/logs.sh -f${NC}   (follow all logs)"
    echo "    ./scripts/setup_local_env/logs.sh web-app    (web app only)"
    echo "    ./scripts/setup_local_env/logs.sh web-ui     (web UI only)"
    echo ""
    echo "  To stop:"
    echo -e "    ${YELLOW}./scripts/setup_local_env/start.sh --clean${NC}  (all services)"
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

    if [[ "$CLEAN" == true ]]; then
        stop_services
        echo ""
    fi

    if [[ "$INFRA" == true ]] || [[ "$DATABASE" == true ]] || [[ "$REDIS" == true ]] || [[ "$MICROSERVICES" == true ]]; then
        start_docker
        echo ""
    fi

    if [[ "$WEB_APP" == true ]]; then
        start_web_app
        echo ""
    fi

    if [[ "$WEB_UI" == true ]]; then
        start_web_ui
        echo ""
    fi

    print_summary

    if [[ "$BLOCK" == true ]]; then
        log_info "Blocking and showing logs (Ctrl+C to stop)..."
        echo ""
        TAIL_PIDS=()
        if [[ "$WEB_APP" == true ]]; then
            echo "--- Web App Log ---"
            tail -f "${LOGS_DIR}/web-app.log" &
            TAIL_PIDS+=($!)
        fi
        if [[ "$WEB_UI" == true ]]; then
            echo "--- Web UI Log ---"
            tail -f "${LOGS_DIR}/web-ui.log" &
            TAIL_PIDS+=($!)
        fi
        if [[ ${#TAIL_PIDS[@]} -gt 0 ]]; then
            wait "${TAIL_PIDS[@]}"
        fi
    fi
}

main "$@"
