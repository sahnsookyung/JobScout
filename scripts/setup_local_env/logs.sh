#!/bin/bash
# =============================================================================
# JobScout - Log Viewing Script
# =============================================================================
# Usage:
#   ./logs.sh                    Show all logs
#   ./logs.sh -f                 Follow all logs in real-time
#   ./logs.sh web-app            Show web app log
#   ./logs.sh web-ui             Show web UI log
#   ./logs.sh microservices      Show all microservice logs
#   ./logs.sh postgres           Show PostgreSQL logs
#   ./logs.sh redis              Show Redis logs
#   ./logs.sh -c                 Clear all logs
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGS_DIR="${SCRIPT_DIR}/logs"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DOCKER_COMPOSE_FILE="${PROJECT_ROOT}/docker-compose.yml"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

LINES=50

get_log_info() {
    local filename="$1"
    case "$filename" in
        web-app)      echo "Web App:${GREEN}";;
        web-ui)       echo "Web UI:${CYAN}";;
        extraction)   echo "Extraction:${YELLOW}";;
        embeddings)   echo "Embeddings:${YELLOW}";;
        scorer-matcher) echo "Scorer-Matcher:${YELLOW}";;
        orchestrator) echo "Orchestrator:${YELLOW}";;
        postgres)     echo "PostgreSQL:${BLUE}";;
        redis)        echo "Redis:${BLUE}";;
        *)            echo "${filename}:${YELLOW}";;
    esac
}

show_help() {
    head -15 "$0" | tail -13
}

print_log() {
    local label="$1"
    local file="$2"
    local color="$3"

    if [[ -f "$file" ]]; then
        echo -e "${color}=== ${label} ===${NC}"
        tail -n ${LINES} "$file"
    else
        echo -e "${YELLOW}[${label}]${NC} Log file not found: $file" >&2
    fi
}

print_log_follow() {
    local label="$1"
    local file="$2"
    local color="$3"

    if [[ -f "$file" ]]; then
        echo -e "${color}=== ${label} ===${NC}"
        tail -n ${LINES} -f "$file"
    fi
}

main() {
    local follow=false
    local clear=false
    local service=""

    while [[ $# -gt 0 ]]; do
        case "$1" in
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
            -*)
                echo -e "${RED}Unknown option: $1${NC}" >&2
                show_help
                exit 1
                ;;
            *)
                service="$1"
                shift
                ;;
        esac
    done

    if [[ ! -d "${LOGS_DIR}" ]]; then
        echo -e "${YELLOW}Logs directory not found: ${LOGS_DIR}${NC}"
        exit 1
    fi

    if [[ "$clear" = true ]]; then
        echo -e "${YELLOW}Clearing all logs...${NC}"
        rm -f "${LOGS_DIR}"/*.log
        echo -e "${GREEN}Logs cleared!${NC}"
        exit 0
    fi

    if [[ -z "$service" ]] || [[ "$service" == "all" ]]; then
        echo -e "${BLUE}=== JobScout All Logs ===${NC}"
        echo ""
        for logfile in "${LOGS_DIR}"/*.log; do
            [[ -f "$logfile" ]] || continue
            filename=$(basename "$logfile" .log)
            info=$(get_log_info "$filename")
            label="${info%%:*}"
            color="${info##*:}"
            if [[ "$follow" = true ]]; then
                print_log_follow "$label" "$logfile" "$color" &
            else
                print_log "$label" "$logfile" "$color"
                echo ""
            fi
        done
        if [[ "$follow" = true ]]; then
            wait
        fi
        return 0
    fi

    case "$service" in
        web-app)
            if [[ "$follow" = true ]]; then
                print_log_follow "Web App" "${LOGS_DIR}/web-app.log" "$GREEN"
            else
                print_log "Web App" "${LOGS_DIR}/web-app.log" "$GREEN"
            fi
            ;;
        web-ui)
            if [[ "$follow" = true ]]; then
                print_log_follow "Web UI" "${LOGS_DIR}/web-ui.log" "$CYAN"
            else
                print_log "Web UI" "${LOGS_DIR}/web-ui.log" "$CYAN"
            fi
            ;;
        microservices|micro)
            for svc in extraction embeddings scorer-matcher orchestrator; do
                logfile="${LOGS_DIR}/${svc}.log"
                if [[ -f "$logfile" ]]; then
                    if [[ "$follow" = true ]]; then
                        print_log_follow "${svc}" "$logfile" "$YELLOW" &
                    else
                        print_log "${svc}" "$logfile" "$YELLOW"
                        echo ""
                    fi
                fi
            done
            if [[ "$follow" = true ]]; then
                wait
            fi
            ;;
        postgres)
            if [[ -f "${DOCKER_COMPOSE_FILE}" ]]; then
                if [[ "$follow" = true ]]; then
                    docker compose -f "${DOCKER_COMPOSE_FILE}" logs -f postgres
                else
                    docker compose -f "${DOCKER_COMPOSE_FILE}" logs --tail=${LINES} postgres
                fi
            else
                echo -e "${RED}docker-compose.yml not found${NC}"
            fi
            ;;
        redis)
            if [[ -f "${DOCKER_COMPOSE_FILE}" ]]; then
                if [[ "$follow" = true ]]; then
                    docker compose -f "${DOCKER_COMPOSE_FILE}" logs -f redis
                else
                    docker compose -f "${DOCKER_COMPOSE_FILE}" logs --tail=${LINES} redis
                fi
            else
                echo -e "${RED}docker-compose.yml not found${NC}"
            fi
            ;;
        infra|infrastructure)
            for svc in postgres redis; do
                if [[ -f "${DOCKER_COMPOSE_FILE}" ]]; then
                    if [[ "$follow" = true ]]; then
                        docker compose -f "${DOCKER_COMPOSE_FILE}" logs -f "$svc" &
                    else
                        echo -e "${BLUE}=== ${svc^} ===${NC}"
                        docker compose -f "${DOCKER_COMPOSE_FILE}" logs --tail=${LINES} "$svc"
                        echo ""
                    fi
                fi
            done
            if [[ "$follow" = true ]]; then
                wait
            fi
            ;;
        docker)
            if [[ -f "${DOCKER_COMPOSE_FILE}" ]]; then
                if [[ "$follow" = true ]]; then
                    docker compose -f "${DOCKER_COMPOSE_FILE}" logs -f
                else
                    docker compose -f "${DOCKER_COMPOSE_FILE}" logs --tail=${LINES}
                fi
            else
                echo -e "${RED}docker-compose.yml not found${NC}"
            fi
            ;;
        *)
            echo -e "${RED}Unknown service: $service${NC}" >&2
            show_help
            exit 1
            ;;
    esac
}

main "$@"
