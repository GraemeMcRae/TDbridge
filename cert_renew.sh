#!/usr/bin/env bash
# cert_renew.sh — Daily certificate health check and renewal for hcf.squadrontrucking.com
#
# Runs daily via cron.  Checks port 80 availability, determines days until
# certificate expiry, performs a dry run (>30 days) or real renewal (≤30 days),
# and logs everything to ~/TDbridge/TDbridge_utility.log with one backup file.
#
# Log rotation: when TDbridge_utility.log reaches 5 MB, it is renamed to
# TDbridge_utility.log.1 (overwriting any previous .log.1) and a new log is started.
#
# Usage: run automatically via cron (see below) or manually:
#   bash ~/TDbridge/cert_renew.sh
#
# Cron entry (runs daily at 3 AM):
#   0 3 * * * /home/graeme/TDbridge/cert_renew.sh

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DOMAIN="hcf.squadrontrucking.com"
CERT_FILE="/etc/letsencrypt/live/${DOMAIN}/fullchain.pem"
ARCHIVE_GLOB="/etc/letsencrypt/archive/${DOMAIN}/*.pem"
LOG_FILE="/home/graeme/TDbridge/TDbridge_utility.log"
LOG_BACKUP="/home/graeme/TDbridge/TDbridge_utility.log.1"
LOG_MAX_BYTES=$((5 * 1024 * 1024))   # 5 MB

# ---------------------------------------------------------------------------
# Logging helper
# ---------------------------------------------------------------------------
log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp
    timestamp=$(TZ="America/Los_Angeles" date '+%Y-%m-%d %H:%M:%S %Z')

    # Rotate if log has reached the size limit
    if [[ -f "$LOG_FILE" ]]; then
        local size
        size=$(stat -c%s "$LOG_FILE" 2>/dev/null || echo 0)
        if (( size >= LOG_MAX_BYTES )); then
            mv "$LOG_FILE" "$LOG_BACKUP"
        fi
    fi

    echo "${timestamp} - ${level} - cert_renew: ${message}" >> "$LOG_FILE"
}

log_info()    { log "INFO"    "$@"; }
log_success() { log "SUCCESS" "$@"; }
log_warning() { log "WARNING" "$@"; }
log_error()   { log "ERROR"   "$@"; }

# ---------------------------------------------------------------------------
# Step 1: Ensure port 80 is free
#
# Webuzo runs a watchdog that restarts httpd independently of systemd.
# Rather than fighting the watchdog, we kill any httpd process holding
# port 80 right before certbot needs it.  certbot only needs port 80 for
# a few seconds; the watchdog will bring httpd back afterward on its own.
# ---------------------------------------------------------------------------
log_info "=== Certificate check starting for ${DOMAIN} ==="

if ss -tlnp 2>/dev/null | grep -q ':80\b'; then
    # Find the process name for logging
    port80_process=$(ss -tlnp | grep ':80\b' | grep -oP 'users:\(\("\K[^"]+' | head -1)
    log_info "Port 80 is in use (process: ${port80_process:-unknown}) — killing it to free port for certbot"

    # Kill all processes listening on port 80
    port80_pids=$(ss -tlnp | grep ':80\b' | grep -oP 'pid=\K[0-9]+' | sort -u)
    if [[ -n "$port80_pids" ]]; then
        for pid in $port80_pids; do
            if ! kill -0 "$pid" 2>/dev/null; then
                # Process already gone (e.g. child exited when parent was killed)
                log_info "pid ${pid} already gone (child of a previously killed parent)"
            else
                sudo kill "$pid" 2>/dev/null                     && log_info "Killed pid ${pid}"                     || log_warning "Could not kill pid ${pid} (permission denied or other error)"
            fi
        done
        # Give the OS a moment to release the port
        sleep 2
    fi

    # Verify port 80 is now free
    if ss -tlnp 2>/dev/null | grep -q ':80\b'; then
        port80_process=$(ss -tlnp | grep ':80\b' | grep -oP 'users:\(\("\K[^"]+' | head -1)
        log_error "Port 80 is still in use (process: ${port80_process:-unknown}) after kill attempt. certbot cannot run."
        exit 1
    fi

    log_info "Port 80 freed successfully — certbot HTTP challenge will work"
else
    log_info "Port 80 is free — certbot HTTP challenge will work"
fi

# ---------------------------------------------------------------------------
# Step 1b: Verify httpd.service is still stopped and disabled
#
# Webuzo's watchdog may attempt to re-enable and start httpd.service via
# systemd.  If it has, stop and disable it again now while we have the chance.
# ---------------------------------------------------------------------------
httpd_active=$(systemctl is-active httpd.service 2>/dev/null)
httpd_enabled=$(systemctl is-enabled httpd.service 2>/dev/null)

if [[ "$httpd_active" == "active" ]]; then
    log_warning "httpd.service is active — Webuzo watchdog may have restarted it. Stopping it now."
    systemctl stop httpd.service 2>/dev/null && log_info "httpd.service stopped" || log_warning "Could not stop httpd.service"
else
    log_info "httpd.service is ${httpd_active} (expected: inactive) — OK"
fi

if [[ "$httpd_enabled" == "enabled" ]]; then
    log_warning "httpd.service is enabled — Webuzo watchdog may have re-enabled it. Disabling it now."
    systemctl disable httpd.service 2>/dev/null && log_info "httpd.service disabled" || log_warning "Could not disable httpd.service"
else
    log_info "httpd.service is ${httpd_enabled} (expected: disabled) — OK"
fi

# ---------------------------------------------------------------------------
# Step 2: Determine days until certificate expiry
# ---------------------------------------------------------------------------
if [[ ! -f "$CERT_FILE" ]]; then
    log_error "Certificate file not found: ${CERT_FILE}"
    exit 1
fi

expiry_date=$(openssl x509 -noout -enddate -in "$CERT_FILE" 2>/dev/null | cut -d= -f2)
if [[ -z "$expiry_date" ]]; then
    log_error "Could not read expiry date from certificate"
    exit 1
fi

expiry_epoch=$(date -d "$expiry_date" +%s 2>/dev/null)
now_epoch=$(date +%s)
days_remaining=$(( (expiry_epoch - now_epoch) / 86400 ))

log_info "Certificate expires: ${expiry_date} (${days_remaining} days from now)"

# ---------------------------------------------------------------------------
# Step 3: Dry run or real renewal depending on days remaining
# ---------------------------------------------------------------------------
if (( days_remaining >= 30 )); then
    # --- Dry run only ---
    log_info "${days_remaining} days until expiry — performing dry run (renewal not yet needed)"

    dry_run_output=$(certbot certonly \
        --standalone \
        --dry-run \
        --non-interactive \
        --agree-tos \
        -d "$DOMAIN" 2>&1)
    dry_run_exit=$?

    if (( dry_run_exit == 0 )); then
        log_success "Dry run succeeded — certificate renewal process is healthy. ${days_remaining} days until expiry."
    else
        log_error "Dry run FAILED (exit ${dry_run_exit}). certbot output follows:"
        while IFS= read -r line; do
            log_error "  ${line}"
        done <<< "$dry_run_output"
        exit 1
    fi

else
    # --- Real renewal ---
    log_info "${days_remaining} days until expiry (<30) — performing real renewal"

    renew_output=$(certbot certonly \
        --standalone \
        --non-interactive \
        --agree-tos \
        -d "$DOMAIN" 2>&1)
    renew_exit=$?

    if (( renew_exit == 0 )); then
        # Fix permissions so the graeme user can read the new cert files
        chmod 640 $ARCHIVE_GLOB 2>/dev/null
        chmod_exit=$?

        # Restart stunnel so it picks up the new certificate
        systemctl restart stunnel4 2>/dev/null
        stunnel_exit=$?

        # Re-read expiry from the newly installed certificate
        new_expiry=$(openssl x509 -noout -enddate -in "$CERT_FILE" 2>/dev/null | cut -d= -f2)
        new_expiry_epoch=$(date -d "$new_expiry" +%s 2>/dev/null)
        new_days=$(( (new_expiry_epoch - now_epoch) / 86400 ))

        if (( chmod_exit != 0 )); then
            log_warning "Renewal succeeded but chmod on archive files failed (exit ${chmod_exit}). Manual fix may be needed."
        fi
        if (( stunnel_exit != 0 )); then
            log_warning "Renewal succeeded but stunnel4 restart failed (exit ${stunnel_exit}). TLS termination may still be using the old certificate."
        fi

        log_success "Certificate renewed successfully. New expiry: ${new_expiry} (${new_days} days from now). stunnel4 restarted."
    else
        log_error "Certificate renewal FAILED (exit ${renew_exit}). certbot output follows:"
        while IFS= read -r line; do
            log_error "  ${line}"
        done <<< "$renew_output"
        log_error "URGENT: Certificate expires in ${days_remaining} days. Manual intervention required."
        exit 1
    fi
fi

log_info "=== Certificate check complete ==="
exit 0
