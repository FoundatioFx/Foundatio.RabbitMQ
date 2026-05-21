#!/bin/bash
# MCP Chaos Testing Tool Runner
# This script implements the MCP tools for chaos testing against Aspire-managed RabbitMQ containers.
# Usage: ./run-chaos-tool.sh <tool_name> <node> [sizeMB]

set -euo pipefail

TOOL="${1:-}"
NODE="${2:-}"
SIZE_MB="${3:-0}"

if [[ -z "$TOOL" || -z "$NODE" ]]; then
    echo "Usage: $0 <tool_name> <node> [sizeMB]"
    echo "Tools: fill_disk, clear_disk, stop_node, start_node, check_alarm, cluster_status"
    echo "Nodes: chaos-1, chaos-2, chaos-3"
    exit 1
fi

if [[ ! "$NODE" =~ ^chaos-[1-3]$ ]]; then
    echo "{\"status\": \"error\", \"message\": \"Invalid node: ${NODE}. Must be chaos-1, chaos-2, or chaos-3\"}"
    exit 1
fi

get_container_id() {
    local name="$1"
    local container_id
    container_id=$(docker ps -q --filter "name=${name}" | head -1)
    if [[ -z "$container_id" ]]; then
        echo "ERROR: Container for '${name}' not found. Is the Aspire AppHost running?" >&2
        exit 1
    fi
    echo "$container_id"
}

get_default_size() {
    case "$1" in
        chaos-1) echo 45 ;;
        chaos-2) echo 70 ;;
        chaos-3) echo 95 ;;
        *) echo 45 ;;
    esac
}

case "$TOOL" in
    fill_disk)
        CONTAINER=$(get_container_id "$NODE")
        docker exec "$CONTAINER" sh -c "rabbitmqctl set_disk_free_limit 999GB" 2>/dev/null
        echo "{\"status\": \"ok\", \"node\": \"${NODE}\", \"action\": \"disk_free_limit set to 999GB (alarm triggered)\"}"
        ;;
    clear_disk)
        CONTAINER=$(get_container_id "$NODE")
        docker exec "$CONTAINER" sh -c "rabbitmqctl set_disk_free_limit 10MB" 2>/dev/null
        echo "{\"status\": \"ok\", \"node\": \"${NODE}\", \"action\": \"disk_free_limit set to 10MB (alarm cleared)\"}"
        ;;
    stop_node)
        CONTAINER=$(get_container_id "$NODE")
        docker kill "$CONTAINER" > /dev/null
        echo "{\"status\": \"ok\", \"node\": \"${NODE}\", \"action\": \"stopped\"}"
        ;;
    start_node)
        CONTAINER=$(docker ps -aq --filter "name=${NODE}" | head -1)
        if [[ -z "$CONTAINER" ]]; then
            echo "{\"status\": \"error\", \"message\": \"Container for '${NODE}' not found\"}"
            exit 1
        fi
        docker start "$CONTAINER" > /dev/null
        echo "{\"status\": \"ok\", \"node\": \"${NODE}\", \"action\": \"started\"}"
        ;;
    check_alarm)
        CONTAINER=$(get_container_id "$NODE")
        OUTPUT=$(docker exec "$CONTAINER" sh -c "rabbitmqctl status --formatter json" 2>/dev/null || echo "error")
        if [[ "$OUTPUT" == "error" ]]; then
            echo "{\"status\": \"error\", \"message\": \"Could not query alarms\"}"
        elif echo "$OUTPUT" | grep -q '"resource":"disk"'; then
            echo "{\"status\": \"ok\", \"node\": \"${NODE}\", \"has_alarm\": true}"
        else
            echo "{\"status\": \"ok\", \"node\": \"${NODE}\", \"has_alarm\": false}"
        fi
        ;;
    cluster_status)
        CONTAINER=$(get_container_id "$NODE")
        OUTPUT=$(docker exec "$CONTAINER" sh -c "rabbitmqctl cluster_status --formatter json" 2>/dev/null || echo "{\"error\": \"could not query cluster\"}")
        echo "$OUTPUT"
        ;;
    *)
        echo "{\"status\": \"error\", \"message\": \"Unknown tool: ${TOOL}\"}"
        exit 1
        ;;
esac
