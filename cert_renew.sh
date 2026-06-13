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
#
# NOTE: Ubuntu's certbot package installs a systemd timer (certbot.timer) that
# runs "certbot renew" twice daily.  This is intentionally left alone.
# The renewal conf file (/etc/letsencrypt/renewal/hcf.squadrontrucking.com.conf)
# specifies "authenticator = manual" and "pref_challs = dns-01", which requires
# a human to add a DNS TXT record.  The systemd timer therefore cannot actually
# renew the certificate and does nothing useful for this domain.  This script
# owns all renewal responsibility.  Do NOT change the conf file to use the
# standalone authenticator, as that would allow the systemd timer to attempt
# renewal without the port 80 checks and logging this script provides.

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
# Step 0: Sleep to avoid colliding with Webuzo's per-minute watchdog cron
#
# Webuzo's crons.php (cron entry "* * * * *") runs every minute and acts as a
# watchdog: it relaunches httpd and reclaims port 80, killing certbot with
# SIGKILL (exit 137) if certbot is holding the port at that moment.
#
# We don't touch Webuzo's cron files (fighting the watchdog on its own turf
# risks unpredictable escalation).  Instead we sleep 20 seconds so that this
# minute's crons.php has finished and gotten out of the way.  certbot then
# runs in the ~40-second gap before the next minute's crons.php fires.
#
# The cron job is also scheduled at 9:02 UTC to avoid the */5 (:00,:05,...)
# and hourly (:01) Webuzo cron jobs, leaving only the every-minute crons.php
# to dodge — which this sleep handles.
# ---------------------------------------------------------------------------
log_info "=== Certificate check starting for ${DOMAIN} ==="
log_info "Sleeping 20 seconds to avoid colliding with Webuzo's per-minute watchdog cron"
sleep 20

# ---------------------------------------------------------------------------
# Step 1: Stop and disable Webuzo's httpd.service
#
# Webuzo runs a watchdog that may re-enable and restart httpd.service via
# systemd.  We stop and disable it FIRST (before freeing port 80), because
# the act of disabling can otherwise race with the watchdog and let httpd
# grab port 80 again after we've freed it.
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
# Step 1b: Free port 80
#
# After httpd.service is stopped and disabled, kill any process still holding
# port 80 (Webuzo's watchdog may have launched httpd directly, bypassing
# systemd).  We retry a few times because the watchdog can relaunch httpd
# between our kill and certbot's bind.  certbot only needs port 80 for a few
# seconds, so once we get a clean window we proceed immediately.
# ---------------------------------------------------------------------------
free_port_80() {
    # Returns 0 if port 80 is free, 1 if still occupied after kill attempt.
    if ! ss -tlnp 2>/dev/null | grep -q ':80\b'; then
        return 0   # already free
    fi
    local proc
    proc=$(ss -tlnp | grep ':80\b' | grep -oP 'users:\(\("\K[^"]+' | head -1)
    log_info "Port 80 is in use (process: ${proc:-unknown}) — killing it"
    local pids
    pids=$(ss -tlnp | grep ':80\b' | grep -oP 'pid=\K[0-9]+' | sort -u)
    for pid in $pids; do
        if ! kill -0 "$pid" 2>/dev/null; then
            log_info "pid ${pid} already gone (child of a previously killed parent)"
        else
            sudo kill "$pid" 2>/dev/null \
                && log_info "Killed pid ${pid}" \
                || log_warning "Could not kill pid ${pid} (permission denied or other error)"
        fi
    done
    sleep 2
    if ss -tlnp 2>/dev/null | grep -q ':80\b'; then
        return 1   # still occupied
    fi
    return 0
}

port80_freed=0
for attempt in 1 2 3; do
    if free_port_80; then
        port80_freed=1
        log_info "Port 80 freed successfully (attempt ${attempt}) — certbot HTTP challenge will work"
        break
    else
        log_warning "Port 80 still in use after attempt ${attempt} — Webuzo watchdog may have relaunched httpd. Retrying."
    fi
done

if (( port80_freed == 0 )); then
    port80_process=$(ss -tlnp | grep ':80\b' | grep -oP 'users:\(\("\K[^"]+' | head -1)
    log_error "Port 80 is still in use (process: ${port80_process:-unknown}) after 3 attempts. certbot cannot run."
    exit 1
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

    # Delete the staging account cache before each dry run.
    # certbot 2.9.0 has a bug: when a valid staging authorization exists from
    # a previous run, certbot deactivates it to force a fresh challenge, but
    # the staging server returns the same deactivated authorization, causing
    # "authorization must be pending".  Deleting the cache ensures certbot
    # registers a fresh staging account with no existing authorizations.
    # This only affects the staging server — the production account cache
    # (used by the real renewal path) is completely separate and untouched.
    rm -rf /etc/letsencrypt/accounts/acme-staging-v02.api.letsencrypt.org 2>/dev/null
    log_info "Cleared staging account cache for clean dry run"

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
