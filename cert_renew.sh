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
    timestamp=$(date '+%Y-%m-%d %H:%M:%S %Z')

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
# Step 1: Check port 80 availability
# ---------------------------------------------------------------------------
log_info "=== Certificate check starting for ${DOMAIN} ==="

if ss -tlnp 2>/dev/null | grep -q ':80\b'; then
    # Something is listening on port 80 — find out what
    port80_process=$(ss -tlnp | grep ':80\b' | grep -oP 'users:\(\("\K[^"]+' | head -1)
    log_error "Port 80 is in use (process: ${port80_process:-unknown}). certbot cannot run. Check whether Webuzo httpd restarted unexpectedly."
    exit 1
fi

log_info "Port 80 is free — certbot HTTP challenge will work"

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
