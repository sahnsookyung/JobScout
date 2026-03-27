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
#   ./start.sh --split                      Split topology (infra + web + microservices)
#   ./logs.sh -f                            Tail all logs in real-time
#
# Options:
#   -i, --infra         Start infrastructure (PostgreSQL, Redis) via Docker
#   -d, --database      Start PostgreSQL only (within Docker)
#   -r, --redis         Start Redis only (within Docker)
#   -a, --web-app       Start FastAPI web application server (port 8080)
#   -u, --web-ui        Start Vite frontend UI dev server (port 5173)
#   -m, --microservices Start pipeline microservices (extraction, embeddings, scorer-matcher, orchestrator)
#      --split          Start split topology (infra + web + microservices)
#   -c, --clean         Stop existing services first
#      --build          Rebuild images before starting (default: use cached images)
#      --dev            Mount source code and enable hot reload on all services
#   -h, --help          Show this help message
#
# Default behavior (no options):
#   Starts: infra + web-app + web-ui + microservices (full stack)
#   This ensures the frontend has data to display and all services are available
#
# Examples:
#   ./start.sh                           Full stack (everything)
#   ./start.sh --split                   Microservices topology
#   ./start.sh --web-app --web-ui        API + UI only (use existing DB + microservices)
#   ./start.sh --infra --microservices   DB + microservices (no web UI)
#   ./start.sh --clean --web-app         Clean start with web app only
# =============================================================================

set -e
export DOCKER_BUILDKIT=1

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"  # Go up 2 levels to project root
LOGS_DIR="${SCRIPT_DIR}/logs"  # Logs stay in scripts/setup_local_env/logs
BACKEND_PORT=8080
FRONTEND_PORT=5173
DOCKER_COMPOSE_FILE="${PROJECT_ROOT}/docker-compose.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    printf "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - %s\n" "$1"
    return 0
}

log_success() {
    printf "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - %s\n" "$1"
    return 0
}

log_warn() {
    printf "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - %s\n" "$1"
    return 0
}

log_error() {
    printf "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - %s\n" "$1" >&2
    return 0
}

# Ensure logs directory exists
ensure_logs_dir() {
    if [[ ! -d "${LOGS_DIR}" ]]; then
        mkdir -p "${LOGS_DIR}"
        log_info "Created logs directory: ${LOGS_DIR}"
    fi
    return 0
}

# Print help message
show_help() {
    head -32 "$0" | tail -28
    return 0
}

# Parse command line arguments
parse_args() {
    # Initialize global variables (removed 'local' to make them global)
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
    SPLIT=false
    BUILD=false
    DEV=false
    local option

    while [[ $# -gt 0 ]]; do
        option="$1"
        case $option in
            # Start everything (all services including microservices)
            --all)
                ALL=true
                shift
                ;;
            --split)
                SPLIT=true
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
            # Rebuild images
            --build)
                BUILD=true
                shift
                ;;
            # Dev mode: volume mounts + hot reload via docker-compose.dev.yml
            --dev)
                DEV=true
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
                log_error "Unknown option: $option" >&2
                show_help
                exit 1
                ;;
        esac
    done

    # Default: start common dev stack (infra + web-app + web-ui + microservices) if nothing specified
    # This ensures the frontend has data to display and all services are available
    if [[ "$SPLIT" == true ]]; then
        INFRA=true
        WEB_APP=true
        WEB_UI=true
        MICROSERVICES=true
    fi

    if [[ "$INFRA" == false ]] && [[ "$DATABASE" == false ]] && [[ "$REDIS" == false ]] && \
       [[ "$WEB_APP" == false ]] && [[ "$WEB_UI" == false ]] && \
       [[ "$MICROSERVICES" == false ]] && [[ "$ALL" == false ]] && \
       [[ "$SPLIT" == false ]]; then
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

    if [[ "$OLLAMA" == true ]]; then
        log_warn "Ignoring --ollama: Ollama should run natively, not in Docker"
    fi
    return 0
}

# Stop existing services
stop_services() {
    log_info "Stopping existing services..."

    # Build compose args to stop all potential services (using array for paths with spaces)
    local compose_args=(-f "${DOCKER_COMPOSE_FILE}")
    if [[ -f "${PROJECT_ROOT}/docker-compose.microservices.yml" ]]; then
        compose_args+=(-f "${PROJECT_ROOT}/docker-compose.microservices.yml")
    fi
    if [[ -f "${PROJECT_ROOT}/docker-compose.web.yml" ]]; then
        compose_args+=(-f "${PROJECT_ROOT}/docker-compose.web.yml")
    fi

    # Stop Docker services
    if docker compose "${compose_args[@]}" ps -q 2>/dev/null | grep -q .; then
        log_info "Stopping Docker services..."
        docker compose "${compose_args[@]}" stop 2>/dev/null || true
        docker compose "${compose_args[@]}" rm -f 2>/dev/null || true
    fi

    # Note: Port killing removed - docker compose handles container lifecycle.
    # If port conflicts occur, run: docker compose -f docker-compose.yml rm -f web-backend

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
    elif [[ "$MICROSERVICES" == true ]]; then
        log_info "Starting split topology (infra + microservices)..."
        INFRA=true
        SERVICES_TO_START="postgres redis jobspy db-migrate extraction embeddings scorer-matcher orchestrator"
    elif [[ "$INFRA" == true ]]; then
        # Start all default infra services
        log_info "Starting all Docker services (postgres, redis)..."
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

    # Add microservices compose file if microservices requested
    if [[ "$MICROSERVICES" == true ]]; then
        if [[ ! -f "${PROJECT_ROOT}/docker-compose.microservices.yml" ]]; then
            log_error "docker-compose.microservices.yml not found at ${PROJECT_ROOT}"
            exit 1
        fi
        compose_files+=(-f "${PROJECT_ROOT}/docker-compose.microservices.yml")
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

    # Add dev overlay if requested
    if [[ "$DEV" == true ]] && [[ -f "${PROJECT_ROOT}/docker-compose.dev.yml" ]]; then
        compose_files+=(-f "${PROJECT_ROOT}/docker-compose.dev.yml")
        log_info "Dev overlay enabled (volume mounts + hot reload)"
    fi

    local build_flag=""
    [[ "$BUILD" == true ]] && build_flag="--build"

    if [[ -n "$SERVICES_TO_START" ]]; then
        docker compose "${compose_files[@]}" up -d ${build_flag} ${SERVICES_TO_START}
    else
        docker compose "${compose_files[@]}" up -d ${build_flag}
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

    # Start background log capture for Docker services
    ensure_logs_dir

    # Capture postgres logs
    if docker compose "${compose_files[@]}" ps postgres 2>/dev/null | grep -q "Up"; then
        docker compose "${compose_files[@]}" logs -f postgres > "${LOGS_DIR}/postgres.log" 2>&1 &
        log_info "Capturing PostgreSQL logs to ${LOGS_DIR}/postgres.log"
    fi

    # Capture microservice logs
    for service in extraction embeddings scorer-matcher orchestrator; do
        if docker compose "${compose_files[@]}" ps $service 2>/dev/null | grep -q "Up"; then
            docker compose "${compose_files[@]}" logs -f $service > "${LOGS_DIR}/${service}.log" 2>&1 &
            log_info "Capturing ${service} logs to ${LOGS_DIR}/${service}.log"
        fi
    done
    return 0
}

run_migrations() {
    log_info "Applying database migrations..."

    if ! command -v uv &> /dev/null; then
        log_error "uv is not installed. Install with: pip install uv" >&2
        return 1
    fi

    (
        cd "${PROJECT_ROOT}" &&
        uv run python -m database.migrate
    )

    log_success "Database migrations applied"
    return 0
}

# Start Web Application (FastAPI backend)
start_web_app() {
    local WEB_APP_PID

    log_info "Starting FastAPI web application..."

    # Check if uv is available
    if ! command -v uv &> /dev/null; then
        log_error "uv is not installed. Install with: pip install uv" >&2
        exit 1
    fi

    # Set microservice URLs for local development (when running web app locally but microservices in Docker)
    # These allow the web backend to communicate with microservices on localhost
    export EXTRACTION_URL=${EXTRACTION_URL:-http://localhost:8081}
    export EMBEDDINGS_URL=${EMBEDDINGS_URL:-http://localhost:8082}
    export SCORER_MATCHER_URL=${SCORER_MATCHER_URL:-http://localhost:8083}
    export ORCHESTRATOR_URL=${ORCHESTRATOR_URL:-http://localhost:8084}

    # Note: Port conflict handling removed - docker compose manages container lifecycle.
    # If port conflicts occur, stop services first: ./scripts/setup_local_env/stop.sh

    # Start web app
    # For backend development with hot reload (bare host only):
    # WEB_DEV=true ./scripts/setup_local_env/start.sh
    if [[ "${WEB_DEV:-false}" == "true" ]]; then
        log_info "Starting web backend locally with hot reload (dev mode)..."
        uv run python -m uvicorn web.backend.app:app --host 127.0.0.1 --reload --port ${BACKEND_PORT} > "${LOGS_DIR}/web-app.log" 2>&1 &
    else
        log_info "Starting web backend via Docker..."
        local web_compose_files=(-f "${PROJECT_ROOT}/docker-compose.yml" -f "${PROJECT_ROOT}/docker-compose.web.yml")
        if [[ "$DEV" == true ]] && [[ -f "${PROJECT_ROOT}/docker-compose.dev.yml" ]]; then
            web_compose_files+=(-f "${PROJECT_ROOT}/docker-compose.dev.yml")
        fi
        local build_flag=""
        [[ "$BUILD" == true ]] && build_flag="--build"
        docker compose "${web_compose_files[@]}" --profile web up -d ${build_flag} web-backend
    fi

    WEB_APP_PID=$!
    log_info "Web application started with PID: ${WEB_APP_PID}"
    log_info "  - Dashboard: http://localhost:${BACKEND_PORT}"
    log_info "  - API Docs: http://localhost:${BACKEND_PORT}/docs"

    # Wait for web app to be ready
    log_info "Waiting for web application to be ready..."
    local i
    for i in {1..30}; do
        if curl -s "http://localhost:${BACKEND_PORT}/health" >/dev/null 2>&1; then
            log_success "Web application is ready!"
            # Start background log capture for Docker web-backend
            if [[ "${WEB_DEV:-false}" != "true" ]]; then
                ensure_logs_dir
                local log_compose_files=(-f "${PROJECT_ROOT}/docker-compose.yml" -f "${PROJECT_ROOT}/docker-compose.web.yml")
                docker compose "${log_compose_files[@]}" logs -f web-backend > "${LOGS_DIR}/web-backend.log" 2>&1 &
                log_info "Capturing web-backend logs to ${LOGS_DIR}/web-backend.log"
            fi
            return 0
        fi
        sleep 1
    done

    log_error "Web application failed to start. Check logs at: ${LOGS_DIR}/web-app.log" >&2
    return 1
}

# Start Web UI (Vite frontend)
start_web_ui() {
    local WEB_UI_PID
    local i

    log_info "Starting Vite web UI..."

    # Check if package.json exists
    if [[ ! -f "${PROJECT_ROOT}/web/frontend/package.json" ]]; then
        log_error "Web UI package.json not found" >&2
        return 1
    fi

    # Note: Port conflict handling removed - docker compose manages container lifecycle.
    # If port conflicts occur, stop services first: ./scripts/setup_local_env/stop.sh

    # Start web UI
    # Check if npm is available
    if ! command -v npm &> /dev/null; then
        log_error "npm is not installed. Install Node.js and npm first." >&2
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

    log_error "Web UI failed to start. Check logs at: ${LOGS_DIR}/web-ui.log" >&2
    return 1
}

# Print status summary
print_summary() {
    echo ""
    echo "============================================================================="
    echo "  JobScout is running!"
    echo "============================================================================="
    echo ""
    if [[ "$MICROSERVICES" == true ]]; then
        printf "  ${GREEN}Topology${NC}:    split (microservices)\n"
    fi
    if [[ "$WEB_UI" == true ]]; then
        printf "  ${GREEN}Web UI${NC}:      http://localhost:${FRONTEND_PORT}\n"
    fi
    if [[ "$WEB_APP" == true ]]; then
        printf "  ${GREEN}Web App${NC}:     http://localhost:${BACKEND_PORT}\n"
        printf "  ${GREEN}API Docs${NC}:    http://localhost:${BACKEND_PORT}/docs\n"
    fi
    if [[ "$MICROSERVICES" == true ]]; then
        printf "  ${GREEN}Microservices:${NC}\n"
        printf "    - Extraction:     http://localhost:8081\n"
        printf "    - Embeddings:     http://localhost:8082\n"
        printf "    - Scorer-Matcher: http://localhost:8083\n"
        printf "    - Orchestrator:   http://localhost:8084\n"
    fi
    echo ""
    printf "  ${GREEN}Log dir${NC}:     ${LOGS_DIR}\n"
    echo ""
    echo "  Logs:"
    if [[ "$WEB_APP" == true ]]; then
        if [[ "${WEB_DEV:-false}" == "true" ]]; then
            printf "    ${BLUE}Web App${NC}:     ${LOGS_DIR}/web-app.log\n"
        else
            printf "    ${BLUE}Web Backend${NC}: ${LOGS_DIR}/web-backend.log\n"
        fi
    fi
    if [[ "$WEB_UI" == true ]]; then
        printf "    ${BLUE}Web UI${NC}:      ${LOGS_DIR}/web-ui.log\n"
    fi
    if [[ "$INFRA" == true ]] || [[ "$DATABASE" == true ]]; then
        printf "    ${BLUE}PostgreSQL${NC}:  ${LOGS_DIR}/postgres.log\n"
    fi
    if [[ "$MICROSERVICES" == true ]]; then
        for _svc in extraction embeddings scorer-matcher orchestrator; do
            printf "    ${BLUE}${_svc}${NC}: ${LOGS_DIR}/${_svc}.log\n"
        done
    fi
    echo ""
    echo "  To view logs in real-time:"
    if [[ "$SPLIT" == true ]] || [[ "$MICROSERVICES" == true ]]; then
        printf "    ${YELLOW}./scripts/setup_local_env/logs.sh --split -f${NC}\n"
    else
        printf "    ${YELLOW}./scripts/setup_local_env/logs.sh -f${NC}\n"
    fi
    echo ""
    echo "  Or tail individual files:"
    printf "    ${YELLOW}tail -f ${LOGS_DIR}/*.log${NC}\n"
    echo ""
    echo "  Or use the logs script:"
    printf "    ${YELLOW}./scripts/setup_local_env/logs.sh -f${NC}            (follow all, auto-detect)\n"
    printf "    ./scripts/setup_local_env/logs.sh web-backend  (web backend only)\n"
    printf "    ./scripts/setup_local_env/logs.sh web-ui       (web UI only)\n"
    echo ""
    echo "  To stop:"
    printf "    ${YELLOW}./scripts/setup_local_env/start.sh --clean${NC}  (all services)\n"
    echo ""
    return 0
}

# Main function
main() {
    parse_args "$@"

    # Set topology-specific log directory
    if [[ "$SPLIT" == true ]] || [[ "$MICROSERVICES" == true ]]; then
        LOGS_DIR="${SCRIPT_DIR}/logs/split"
    fi

    ensure_logs_dir

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

    if [[ "$DATABASE" == true ]] || [[ "$INFRA" == true ]] || [[ "$WEB_APP" == true ]] || [[ "$MICROSERVICES" == true ]]; then
        run_migrations
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
        local TAIL_PIDS=()
        if [[ "$WEB_APP" == true ]]; then
            if [[ "${WEB_DEV:-false}" == "true" ]]; then
                echo "--- Web App Log ---"
                tail -f "${LOGS_DIR}/web-app.log" &
            else
                echo "--- Web Backend Log ---"
                tail -f "${LOGS_DIR}/web-backend.log" &
            fi
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
    return 0
}

main "$@"
