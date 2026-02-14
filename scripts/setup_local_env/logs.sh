#!/bin/bash
# =============================================================================
# JobScout - Log Viewing Script
# =============================================================================
# Usage:
#   ./logs.sh              Show all logs (last 50 lines each)
#   ./logs.sh -f           Follow all logs in real-time
#   ./logs.sh backend      Show backend log only
#   ./logs.sh backend -f   Follow backend log only
#   ./logs.sh frontend     Show frontend log only
#   ./logs.sh frontend -f  Follow frontend log only
#   ./logs.sh docker       Show Docker service logs
#   ./logs.sh postgres     Show PostgreSQL logs
#   ./logs.sh redis        Show Redis logs
#   ./logs.sh -c           Clear all logs
#
# Options:
#   -f, --follow    Follow logs in real-time (tail -f)
#   -c, --clear     Clear all log files
#   -h, --help      Show this help message
# =============================================================================

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}"
LOGS_DIR="${PROJECT_ROOT}/logs"
DOCKER_COMPOSE_FILE="${PROJECT_ROOT}/docker-compose.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default lines to show
LINES=50

# Print help message
show_help() {
    head -22 "$0" | tail -18
}

# Print log file with header
print_log() {
    local label=$1
    local file=$2
    local color=$3

    if [ -f "$file" ]; then
        echo -e "${color}=== ${label} ===${NC}"
        if [ "$1" = "follow" ]; then
            tail -n ${LINES} -f "$file"
        else
            tail -n ${LINES} "$file"
        fi
    else
        echo -e "${YELLOW}[${label}]${NC} Log file not found: $file"
    fi
}

# List all logs
list_logs() {
    echo -e "${BLUE}Available logs:${NC}"
    echo ""

    if [ -d "${LOGS_DIR}" ]; then
        echo -e "  ${GREEN}Application Logs:${NC}"
        ls -lh "${LOGS_DIR}"/*.log 2>/dev/null | awk '{print "    " $9 " (" $5 ")"}' || echo "    (empty)"
    else
        echo -e "  ${YELLOW}Logs directory not found: ${LOGS_DIR}${NC}"
    fi

    echo ""
    echo -e "  ${CYAN}Docker Logs:${NC}"
    echo "    Run: ${YELLOW}docker-compose logs${NC}"
    echo "    Or: ${YELLOW}./logs.sh docker${NC}"
}

# Main function
main() {
    local follow=false
    local clear=false
    local service="all"

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -f|--follow)
                follow=true
                shift
                ;;
            -c|--clear)
                clear=true
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            backend|frontend|docker|postgres|redis)
                service=$1
                shift
                ;;
            -*)
                echo -e "${RED}Unknown option: $1${NC}"
                show_help
                exit 1
                ;;
            *)
                shift
                ;;
        esac
    done

    # Ensure logs directory exists
    if [ ! -d "${LOGS_DIR}" ]; then
        echo -e "${YELLOW}No logs directory found: ${LOGS_DIR}${NC}"
        exit 1
    fi

    # Clear logs if requested
    if [ "$clear" = true ]; then
        echo -e "${YELLOW}Clearing all logs...${NC}"
        rm -f "${LOGS_DIR}"/*.log
        echo -e "${GREEN}Logs cleared!${NC}"
        exit 0
    fi

    # Handle different services
    case $service in
        all)
            echo -e "${BLUE}=== JobScout All Logs ===${NC}"
            echo ""
            if [ "$follow" = true ]; then
                # Follow all logs using tail with multiple files
                tail -n ${LINES} -f "${LOGS_DIR}"/*.log 2>/dev/null || {
                    echo -e "${YELLOW}No log files found${NC}"
                }
            else
                # Show last 50 lines of each log
                for logfile in "${LOGS_DIR}"/*.log; do
                    if [ -f "$logfile" ]; then
                        filename=$(basename "$logfile")
                        case "$filename" in
                            backend.log)
                                print_log "Backend" "$logfile" "$GREEN"
                                echo ""
                                ;;
                            frontend.log)
                                print_log "Frontend" "$logfile" "$CYAN"
                                echo ""
                                ;;
                            *)
                                print_log "$filename" "$logfile" "$YELLOW"
                                echo ""
                                ;;
                        esac
                    fi
                done
            fi
            echo ""
            echo -e "${YELLOW}Tip: Use ${CYAN}./logs.sh docker${YELLOW} to view Docker service logs${NC}"
            ;;

        backend)
            if [ "$follow" = true ]; then
                print_log "Backend" "${LOGS_DIR}/backend.log" "$GREEN"
            else
                if [ -f "${LOGS_DIR}/backend.log" ]; then
                    tail -n ${LINES} "${LOGS_DIR}/backend.log"
                else
                    echo -e "${YELLOW}Backend log not found: ${LOGS_DIR}/backend.log${NC}"
                    echo "Is the backend running?"
                fi
            fi
            ;;

        frontend)
            if [ "$follow" = true ]; then
                print_log "Frontend" "${LOGS_DIR}/frontend.log" "$CYAN"
            else
                if [ -f "${LOGS_DIR}/frontend.log" ]; then
                    tail -n ${LINES} "${LOGS_DIR}/frontend.log"
                else
                    echo -e "${YELLOW}Frontend log not found: ${LOGS_DIR}/frontend.log${NC}"
                    echo "Is the frontend running?"
                fi
            fi
            ;;

        docker)
            if [ -f "${DOCKER_COMPOSE_FILE}" ]; then
                if [ "$follow" = true ]; then
                    docker-compose -f "${DOCKER_COMPOSE_FILE}" logs -f
                else
                    docker-compose -f "${DOCKER_COMPOSE_FILE}" logs --tail=${LINES}
                fi
            else
                echo -e "${RED}docker-compose.yml not found${NC}"
            fi
            ;;

        postgres)
            if [ -f "${DOCKER_COMPOSE_FILE}" ]; then
                if [ "$follow" = true ]; then
                    docker-compose -f "${DOCKER_COMPOSE_FILE}" logs -f postgres
                else
                    docker-compose -f "${DOCKER_COMPOSE_FILE}" logs --tail=${LINES} postgres
                fi
            else
                echo -e "${RED}docker-compose.yml not found${NC}"
            fi
            ;;

        redis)
            if [ -f "${DOCKER_COMPOSE_FILE}" ]; then
                if [ "$follow" = true ]; then
                    docker-compose -f "${DOCKER_COMPOSE_FILE}" logs -f redis
                else
                    docker-compose -f "${DOCKER_COMPOSE_FILE}" logs --tail=${LINES} redis
                fi
            else
                echo -e "${RED}docker-compose.yml not found${NC}"
            fi
            ;;
    esac
}

main "$@"
