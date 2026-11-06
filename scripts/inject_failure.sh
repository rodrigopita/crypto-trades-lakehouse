#!/usr/bin/env bash
# Scripted network failures for the cp1 session, run against the ingester container.
#
#   scripts/inject_failure.sh reset <seconds>   terminated connection: TCP resets on port 443
#   scripts/inject_failure.sh stall <seconds>   silent stall: packets on port 443 dropped
#
# A sidecar joins the ingester's network namespace with NET_ADMIN, adds an iptables rule,
# holds it for <seconds> and removes it, also when interrupted. `ss -K` cannot do the reset
# on this WSL kernel (CONFIG_INET_DIAG_DESTROY is not set), so the rules stand in for it.
# INGESTER_CONTAINER overrides the target, which defaults to the compose `ingester` service.
set -euo pipefail

ALPINE_IMAGE="alpine:3.24.3"
usage="usage: $0 reset|stall <seconds>"

mode="${1:?$usage}"
seconds="${2:?$usage}"

case "$mode" in
    reset) rules=("OUTPUT -p tcp --dport 443 -j REJECT --reject-with tcp-reset") ;;
    stall) rules=("OUTPUT -p tcp --dport 443 -j DROP" "INPUT -p tcp --sport 443 -j DROP") ;;
    *) echo "$usage" >&2; exit 2 ;;
esac

container="${INGESTER_CONTAINER:-$(docker compose ps -q ingester)}"
if [ -z "$container" ]; then
    echo "the ingester service is not running" >&2
    exit 1
fi

docker run --rm -i --network "container:$container" --cap-add NET_ADMIN "$ALPINE_IMAGE" \
    sh -s -- "$mode" "$seconds" "${rules[@]}" <<'SIDECAR'
mode=$1
seconds=$2
shift 2

apk add --quiet iptables >/dev/null

remove_rules() {
    for rule in "$@"; do
        # Word splitting is intended: each rule is a list of iptables arguments.
        iptables -D $rule 2>/dev/null || true
    done
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $mode off"
}
trap 'remove_rules "$@"; exit 0' INT TERM

for rule in "$@"; do
    iptables -A $rule
done
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $mode on for ${seconds}s"

# Backgrounded so a trapped signal interrupts the wait instead of waiting out the sleep.
sleep "$seconds" &
wait $!
remove_rules "$@"
SIDECAR
