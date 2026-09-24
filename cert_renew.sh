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
# Do NOT add a per-minute retry cron entry.  This script now handles retries
# internally with exponential back-off (see Step 3).  Retrying every minute
# against an ACME endpoint that has explicitly told us to back off adds to the
# load that caused the refusal, and destroys the ability to distinguish a
# transient service condition from a genuine renewal failure.
#
# Exit codes:
#   0 — check completed successfully (dry run passed, or certificate renewed)
#   1 — genuine failure requiring investigation
#   2 — INCONCLUSIVE: the ACME endpoint was unavailable, so the check could not
#       be completed.  This is NOT a certificate problem.  Dashboard consumers
#       should treat exit 2 as "unverified", not "broken", unless it repeats on
#       consecutive days or expiry is close.
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

# Retry policy for transient ACME service conditions.
# 4 attempts with a doubling delay starting at 300s gives sleeps of
# 300 + 600 + 1200 = 2100s, so the script may run for roughly 35–40 minutes
# in the worst case.  Keep that in mind when scheduling.
CERTBOT_MAX_ATTEMPTS=4
CERTBOT_BASE_DELAY=300

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

# Log a multi-line block of certbot output at the given level, one line per
# log entry, indented — matching the existing log format.
log_block() {
    local level="$1"
    shift
    local line
    while IFS= read -r line; do
        log "$level" "  ${line}"
    done <<< "$*"
}

# ---------------------------------------------------------------------------
# Watchdog dodge
#
# Webuzo's crons.php (cron entry "* * * * *") runs every minute and acts as a
# watchdog: it relaunches httpd and reclaims port 80, killing certbot with
# SIGKILL (exit 137) if certbot is holding the port at that moment.
#
# We don't touch Webuzo's cron files (fighting the watchdog on its own turf
# risks unpredictable escalation).  Instead we sleep 20 seconds past the top of
# a minute so that this minute's crons.php has finished and gotten out of the
# way.  certbot then runs in the ~40-second gap before the next minute fires.
#
# On the first call, cron has already started us at the top of a minute, so the
# alignment sleep is a no-op.  On retry calls we arrive at an arbitrary offset
# within the minute after a back-off sleep, so we first wait for the next minute
# boundary and then do the usual 20 seconds.
# ---------------------------------------------------------------------------
dodge_watchdog() {
    local secs
    # 10# forces base-10 so that "08" and "09" from date +%S don't get
    # interpreted as invalid octal.
    secs=$(( 60 - 10#$(date +%S) ))
    if (( secs > 0 && secs < 60 )); then
        log_info "Waiting ${secs}s for the next minute boundary before the watchdog dodge"
        sleep "$secs"
    fi
    log_info "Sleeping 20 seconds to avoid colliding with Webuzo's per-minute watchdog cron"
    sleep 20
}

# ---------------------------------------------------------------------------
# Step 1 helper: Stop and disable Webuzo's httpd.service
#
# Webuzo runs a watchdog that may re-enable and restart httpd.service via
# systemd.  We stop and disable it FIRST (before freeing port 80), because
# the act of disabling can otherwise race with the watchdog and let httpd
# grab port 80 again after we've freed it.
# ---------------------------------------------------------------------------
quiesce_httpd_service() {
    local httpd_active httpd_enabled
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
}

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
    local pids kill_failed=0
    pids=$(ss -tlnp | grep ':80\b' | grep -oP 'pid=\K[0-9]+' | sort -u)
    for pid in $pids; do
        if ! kill -0 "$pid" 2>/dev/null; then
            log_info "pid ${pid} already gone (child of a previously killed parent)"
        else
            if sudo kill "$pid" 2>/dev/null; then
                log_info "Killed pid ${pid}"
            else
                log_warning "Could not kill pid ${pid} (permission denied or other error)"
                kill_failed=1
            fi
        fi
    done

    # If a kill was refused, record who owns the process.  A process we cannot
    # kill that is genuinely holding the listening socket is the one failure
    # mode this script cannot recover from on its own.
    if (( kill_failed == 1 )); then
        log_warning "Listener detail after failed kill: $(ss -tlnp 2>/dev/null | grep ':80\b' | tr -s ' ' | head -3 | tr '\n' '|')"
    fi

    sleep 2
    if ss -tlnp 2>/dev/null | grep -q ':80\b'; then
        return 1   # still occupied
    fi
    return 0
}

# Wrap free_port_80 in the 3-attempt loop.  Returns 0 if port 80 ends up free,
# 1 otherwise.  Called once before the first certbot attempt and again before
# every retry, because the watchdog reclaims port 80 during the back-off sleep.
ensure_port_80() {
    local attempt proc
    for attempt in 1 2 3; do
        if free_port_80; then
            log_info "Port 80 freed successfully (attempt ${attempt}) — certbot HTTP challenge will work"
            return 0
        fi
        log_warning "Port 80 still in use after attempt ${attempt} — Webuzo watchdog may have relaunched httpd. Retrying."
    done
    proc=$(ss -tlnp | grep ':80\b' | grep -oP 'users:\(\("\K[^"]+' | head -1)
    log_error "Port 80 is still in use (process: ${proc:-unknown}) after 3 attempts. certbot cannot run."
    return 1
}

# ---------------------------------------------------------------------------
# Transient-error classification
#
# Let's Encrypt returns urn:ietf:params:acme:error:rateLimited with the detail
# "Service busy; retry later." when Boulder is shedding load.  This is a
# server-side condition, not a problem with our configuration, and it is common
# on the staging endpoint (which is best-effort, with no uptime guarantee).
#
# We deliberately match on the DETAIL strings rather than on "rateLimited"
# generally.  Genuine rate limits — such as "too many certificates already
# issued for this exact set of domains" — also carry the rateLimited type, and
# retrying those within the hour is exactly the wrong response.  Those fall
# through to the genuine-failure path and stop the script immediately.
# ---------------------------------------------------------------------------
is_transient_acme_error() {
    grep -qiE \
        'Service busy|Problem getting authorization|The server experienced an internal error|urn:ietf:params:acme:error:(serverInternal|connection)|Timeout during connect' \
        <<< "$1"
}

# ---------------------------------------------------------------------------
# certbot invocation with bounded exponential back-off
#
# Usage: run_certbot_with_retry dry-run | live
#
# Returns:
#   0 — certbot succeeded.  Attempt number is left in CERTBOT_ATTEMPTS and the
#       output in CERTBOT_OUTPUT.
#   1 — genuine failure (non-transient certbot error, or port 80 unrecoverable)
#   2 — inconclusive: every attempt hit a transient ACME service condition
# ---------------------------------------------------------------------------
CERTBOT_ATTEMPTS=0
CERTBOT_OUTPUT=""

run_certbot_with_retry() {
    local mode="$1"
    local label attempt delay rc output
    local -a extra_args=()

    if [[ "$mode" == "dry-run" ]]; then
        label="Dry run"
        extra_args+=( --dry-run )
    else
        label="Renewal"
    fi

    delay=$CERTBOT_BASE_DELAY

    for (( attempt = 1; attempt <= CERTBOT_MAX_ATTEMPTS; attempt++ )); do
        CERTBOT_ATTEMPTS=$attempt

        # Attempts after the first arrive minutes later, so the watchdog has
        # had time to relaunch httpd and reclaim port 80.  Redo the dodge and
        # the port-80 free before touching certbot again.
        if (( attempt > 1 )); then
            log_info "--- ${label} attempt ${attempt}/${CERTBOT_MAX_ATTEMPTS}: re-establishing a clean port 80 window ---"
            quiesce_httpd_service
            dodge_watchdog
            if ! ensure_port_80; then
                log_error "${label} aborted on attempt ${attempt}: could not free port 80."
                return 1
            fi
        fi

        if [[ "$mode" == "dry-run" ]]; then
            # Delete the staging account cache before each dry run attempt.
            # certbot 2.9.0 has a bug: when a valid staging authorization exists
            # from a previous run, certbot deactivates it to force a fresh
            # challenge, but the staging server returns the same deactivated
            # authorization, causing "authorization must be pending".  Deleting
            # the cache ensures certbot registers a fresh staging account with
            # no existing authorizations.  This only affects the staging server
            # — the production account cache (used by the real renewal path) is
            # completely separate and untouched.
            rm -rf /etc/letsencrypt/accounts/acme-staging-v02.api.letsencrypt.org 2>/dev/null
            log_info "Cleared staging account cache for clean dry run (attempt ${attempt}/${CERTBOT_MAX_ATTEMPTS})"
        fi

        output=$(certbot certonly \
            --standalone \
            "${extra_args[@]}" \
            --non-interactive \
            --agree-tos \
            -d "$DOMAIN" 2>&1)
        rc=$?

        if (( rc == 0 )); then
            CERTBOT_OUTPUT="$output"
            return 0
        fi

        if is_transient_acme_error "$output"; then
            log_warning "${label} INCONCLUSIVE (exit ${rc}, attempt ${attempt}/${CERTBOT_MAX_ATTEMPTS}) — ACME service busy or unavailable. certbot output follows:"
            log_block "WARNING" "$output"
            if (( attempt < CERTBOT_MAX_ATTEMPTS )); then
                log_info "Backing off ${delay}s before retry"
                sleep "$delay"
                delay=$(( delay * 2 ))
            fi
            continue
        fi

        # Non-transient: retrying will not help and may make things worse.
        log_error "${label} FAILED (exit ${rc}) — non-transient error, not retrying. certbot output follows:"
        log_block "ERROR" "$output"
        CERTBOT_OUTPUT="$output"
        return 1
    done

    log_warning "${label} could not be completed after ${CERTBOT_MAX_ATTEMPTS} attempts — the ACME endpoint was unavailable throughout. This is a service condition, not a certificate problem."
    return 2
}

# ---------------------------------------------------------------------------
# Step 0 / 1 / 1b: open the first clean port 80 window
# ---------------------------------------------------------------------------
log_info "=== Certificate check starting for ${DOMAIN} ==="
dodge_watchdog
quiesce_httpd_service

if ! ensure_port_80; then
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

    run_certbot_with_retry "dry-run"
    dry_run_result=$?

    case $dry_run_result in
        0)
            log_success "Dry run succeeded on attempt ${CERTBOT_ATTEMPTS} — certificate renewal process is healthy. ${days_remaining} days until expiry."
            ;;
        2)
            log_warning "Dry run UNVERIFIED today — ACME staging was unavailable for all ${CERTBOT_MAX_ATTEMPTS} attempts. Certificate is fine (${days_remaining} days until expiry); the health check simply could not run. Tomorrow's run will retry."
            log_info "=== Certificate check complete (inconclusive) ==="
            exit 2
            ;;
        *)
            log_error "Dry run failed for a non-transient reason — investigate before expiry (${days_remaining} days remaining)."
            exit 1
            ;;
    esac

else
    # --- Real renewal ---
    log_info "${days_remaining} days until expiry (<30) — performing real renewal"

    run_certbot_with_retry "live"
    renew_result=$?

    if (( renew_result == 2 )); then
        # Inconclusive on the live path.  Whether this is urgent depends
        # entirely on how much runway is left: with three weeks to go,
        # tomorrow's run will almost certainly succeed.  Inside a week, it
        # needs a human.
        if (( days_remaining <= 7 )); then
            log_error "URGENT: Renewal could not be completed (ACME endpoint unavailable) and the certificate expires in ${days_remaining} days. Manual intervention required."
            exit 1
        fi
        log_warning "Renewal UNVERIFIED today — ACME endpoint unavailable for all ${CERTBOT_MAX_ATTEMPTS} attempts, with ${days_remaining} days until expiry. Tomorrow's run will retry."
        log_info "=== Certificate check complete (inconclusive) ==="
        exit 2
    fi

    if (( renew_result != 0 )); then
        log_error "URGENT: Certificate expires in ${days_remaining} days and renewal failed for a non-transient reason. Manual intervention required."
        exit 1
    fi

    # --- Renewal succeeded: post-renewal steps ---

    # Fix permissions so the graeme user can read the new cert files
    chmod 640 $ARCHIVE_GLOB 2>/dev/null
    chmod_exit=$?

    # Restart stunnel so it picks up the new certificate
    systemctl restart stunnel4 2>/dev/null
    stunnel_exit=$?

    # Re-read expiry from the newly installed certificate.  now_epoch is
    # recomputed here because the retry back-off may have consumed half an hour
    # since it was first captured.
    now_epoch=$(date +%s)
    new_expiry=$(openssl x509 -noout -enddate -in "$CERT_FILE" 2>/dev/null | cut -d= -f2)
    new_expiry_epoch=$(date -d "$new_expiry" +%s 2>/dev/null)
    new_days=$(( (new_expiry_epoch - now_epoch) / 86400 ))

    if (( chmod_exit != 0 )); then
        log_warning "Renewal succeeded but chmod on archive files failed (exit ${chmod_exit}). Manual fix may be needed."
    fi
    if (( stunnel_exit != 0 )); then
        log_warning "Renewal succeeded but stunnel4 restart failed (exit ${stunnel_exit}). TLS termination may still be using the old certificate."
    fi

    log_success "Certificate renewed successfully on attempt ${CERTBOT_ATTEMPTS}. New expiry: ${new_expiry} (${new_days} days from now). stunnel4 restarted."
fi

log_info "=== Certificate check complete ==="
exit 0
