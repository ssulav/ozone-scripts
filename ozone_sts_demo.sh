#!/usr/bin/env bash
#
# Interactive demo: Ozone STS + Ranger AssumeRole setup
#
# Pauses before each step. Press Enter to run a command, or:
#   s + Enter  skip the command
#   q + Enter  quit
#
# Resume from a step after a partial run:
#   START_STEP=6 DEMO_RUN_ID=12345 ./ozone_sts_demo.sh
# State is saved to .ozone-sts-demo-<DEMO_RUN_ID>.env after steps 3-6.
#
# Ranger curl steps: shows the curl command, runs it in this same pane
# when you press Enter, prints the response, then waits before continuing.
#
# Override defaults via environment variables, e.g.:
#   RANGER_HOST=... OZONE_S3G_HOST=... \
#   RANGER_ADMIN_USER=admin RANGER_ADMIN_PASS=secret \
#   ./ozone_sts_demo.sh
#
# Run all commands without prompts:
#   AUTO_RUN=1 ./ozone_sts_demo.sh

set -euo pipefail

_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---------------------------------------------------------------------------
# Configuration (edit or export before running)
# ---------------------------------------------------------------------------
RANGER_HOST="${RANGER_HOST:-ccycloud-1.fb81615635-bha.root.comops.site}"
OZONE_S3G_HOST="${OZONE_S3G_HOST:-ccycloud-1.fb81615635-01.root.comops.site}"
RANGER_PORT="${RANGER_PORT:-6182}"
OZONE_S3G_PORT="${OZONE_S3G_PORT:-9879}"
OZONE_STS_PORT="${OZONE_STS_PORT:-9881}"

RANGER_ADMIN_USER="${RANGER_ADMIN_USER:-admin}"
RANGER_ADMIN_PASS="${RANGER_ADMIN_PASS:-Admin123}"

SVC_PASSWORD="${SVC_PASSWORD:-Password123}"
HDFS_KEYTAB="${HDFS_KEYTAB:-/cdep/keytabs/hdfs.keytab}"
KERB_REALM="${KERB_REALM:-ROOT.COMOPS.SITE}"
HDFS_HOST="${HDFS_HOST:-$(hostname -f)}"
HDFS_PRINCIPAL="${HDFS_PRINCIPAL:-hdfs/${HDFS_HOST}@${KERB_REALM}}"

# Unique 5-digit suffix each run (override with DEMO_RUN_ID=12345 to replay)
DEMO_RUN_ID="${DEMO_RUN_ID:-$(printf '%05d' $(( (RANDOM + $$) % 100000 )))}"
SVC_USER="${SVC_USER:-rest-catalog-${DEMO_RUN_ID}}"
SVC_ROLE="${SVC_ROLE:-iceberg-${DEMO_RUN_ID}}"
ASSUME_POLICY_NAME="${ASSUME_POLICY_NAME:-assume-${DEMO_RUN_ID}}"
VOLUME_POLICY_NAME="${VOLUME_POLICY_NAME:-vol-${DEMO_RUN_ID}}"
BUCKET_POLICY_NAME="${BUCKET_POLICY_NAME:-bkt-${DEMO_RUN_ID}}"
KEY_POLICY_NAME="${KEY_POLICY_NAME:-key-${DEMO_RUN_ID}}"
SVC_KERB_PRINCIPAL="${SVC_KERB_PRINCIPAL:-${SVC_USER}/cli@${KERB_REALM}}"
SVC_KEYTAB="${SVC_KEYTAB:-/cdep/keytabs/${SVC_USER}.keytab}"

BUCKET="${BUCKET:-bkt-${DEMO_RUN_ID}}"
OZONE_VOLUME="${OZONE_VOLUME:-s3v}"
DEMO_OBJECT="${DEMO_OBJECT:-demo-${DEMO_RUN_ID}.txt}"
LOCAL_DEMO_FILE="${LOCAL_DEMO_FILE:-./demotest.txt}"
DOWNLOAD_FILE="${DOWNLOAD_FILE:-./test.txt}"

RANGER_BASE="https://${RANGER_HOST}:${RANGER_PORT}"
OZONE_STS_ENDPOINT="https://${OZONE_S3G_HOST}:${OZONE_STS_PORT}/sts"
OZONE_S3G_ENDPOINT="https://${OZONE_S3G_HOST}:${OZONE_S3G_PORT}"

STEP=0
STEP_COUNTER=0
ACTIVE_STEP=1
AUTO_RUN="${AUTO_RUN:-0}"
START_STEP="${START_STEP:-1}"
STATE_FILE="${STATE_FILE:-${_SCRIPT_DIR}/.ozone-sts-demo-${DEMO_RUN_ID}.env}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
banner() {
  echo
  echo "================================================================================"
  echo "$1"
  echo "================================================================================"
}

step_header() {
  STEP_COUNTER=$((STEP_COUNTER + 1))
  if (( STEP_COUNTER < START_STEP )); then
    say "[skipped] Step ${STEP_COUNTER}: $1"
    ACTIVE_STEP=0
    return 0
  fi
  ACTIVE_STEP=1
  STEP=${STEP_COUNTER}
  banner "Step ${STEP}: $1"
}

save_demo_state() {
  {
    printf 'DEMO_RUN_ID=%q\n' "${DEMO_RUN_ID}"
    printf 'SVC_USER=%q\n' "${SVC_USER}"
    printf 'SVC_ROLE=%q\n' "${SVC_ROLE}"
    printf 'ASSUME_POLICY_NAME=%q\n' "${ASSUME_POLICY_NAME}"
    printf 'VOLUME_POLICY_NAME=%q\n' "${VOLUME_POLICY_NAME}"
    printf 'BUCKET_POLICY_NAME=%q\n' "${BUCKET_POLICY_NAME}"
    printf 'KEY_POLICY_NAME=%q\n' "${KEY_POLICY_NAME}"
    printf 'BUCKET=%q\n' "${BUCKET}"
    printf 'DEMO_OBJECT=%q\n' "${DEMO_OBJECT}"
    printf 'SVC_KERB_PRINCIPAL=%q\n' "${SVC_KERB_PRINCIPAL}"
    printf 'SVC_KEYTAB=%q\n' "${SVC_KEYTAB}"
  } > "${STATE_FILE}"
  say "Saved demo state to ${STATE_FILE}"
}

load_demo_state() {
  if [[ ! -f "${STATE_FILE}" ]]; then
    echo "No state file at ${STATE_FILE} — set DEMO_RUN_ID and vars manually or run from step 1." >&2
    return 1
  fi
  # shellcheck disable=SC1090
  if ! source "${STATE_FILE}"; then
    echo "Failed to load state file ${STATE_FILE} — delete it or fix invalid lines, then re-run." >&2
    return 1
  fi
  echo "Loaded demo state from ${STATE_FILE}"
  printf '  SVC_USER=%s\n' "${SVC_USER}"
  printf '  SVC_ROLE=%s\n' "${SVC_ROLE}"
  printf '  ASSUME_POLICY_NAME=%s\n' "${ASSUME_POLICY_NAME}"
  printf '  VOLUME_POLICY_NAME=%s\n' "${VOLUME_POLICY_NAME}"
  printf '  BUCKET_POLICY_NAME=%s\n' "${BUCKET_POLICY_NAME}"
  printf '  KEY_POLICY_NAME=%s\n' "${KEY_POLICY_NAME}"
  printf '  BUCKET=%s\n' "${BUCKET}"
  printf '  DEMO_OBJECT=%s\n' "${DEMO_OBJECT}"
  return 0
}

policy_section_header() {
  [[ "${ACTIVE_STEP}" == "1" ]] || return 0
  local kind="$1"
  local policy_name="$2"
  local scope="$3"
  echo
  echo "################################################################################"
  echo "#  RANGER DATA ACCESS POLICY — ${kind}"
  echo "#  Policy name: ${policy_name}"
  echo "#  Role:        ${SVC_ROLE}"
  echo "#  Scope:       ${scope}"
  echo "################################################################################"
  echo
}

say() {
  [[ "${ACTIVE_STEP}" == "1" ]] || return 0
  echo "$*"
}

read_choice() {
  if [[ "${AUTO_RUN}" == "1" ]]; then
    return 0
  fi
  local prompt="$1"
  local choice=""
  if [[ -r /dev/tty ]]; then
    read -r -p "${prompt}" choice </dev/tty || true
  else
    read -r -p "${prompt}" choice || true
  fi
  printf '%s' "${choice}" | tr '[:upper:]' '[:lower:]'
}

prompt_continue() {
  local prompt="${1:-Press Enter to run, s=skip, q=quit: }"
  local choice
  choice="$(read_choice "${prompt}")"
  case "${choice}" in
    q) echo "Demo stopped."; exit 0 ;;
    s) return 1 ;;
    r|*) return 0 ;;
  esac
}

json_body_inline() {
  local body_file="$1"
  tr -d '\n' < "${body_file}" | sed 's/  */ /g'
}

show_ranger_rest_display() {
  local method="$1"
  local url="$2"
  local body_file="${3:-}"
  say "curl -k -u \"\${RANGER_ADMIN_USER}:\${RANGER_ADMIN_PASS}\" \\"
  say "  --include --location \\"
  say "  --request ${method} \\"
  say "  --header 'accept: application/json' \\"
  if [[ -n "${body_file}" && -f "${body_file}" ]]; then
    say "  --header 'Content-Type: application/json' \\"
    say "  --data '$(json_body_inline "${body_file}")' \\"
  fi
  say "  '${url}'"
}

exec_ranger_rest() {
  local method="$1"
  local url="$2"
  local body_file="${3:-}"
  if [[ -n "${body_file}" && -f "${body_file}" ]]; then
    curl -k -u "${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASS}" \
      --include --location \
      --request "${method}" \
      --header "Content-Type: application/json" \
      --header "accept: application/json" \
      --data @"${body_file}" \
      "${url}"
  else
    curl -k -u "${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASS}" \
      --include --location \
      --request "${method}" \
      --header "accept: application/json" \
      "${url}"
  fi
}

run_ranger_rest() {
  if [[ "${ACTIVE_STEP}" != "1" ]]; then
    return 0
  fi

  local method="$1"
  local url="$2"
  local body_file="${3:-}"

  if [[ -n "${body_file}" && -f "${body_file}" ]]; then
    say
    say "JSON body:"
    sed 's/^/  /' "${body_file}"
    say
  fi

  say "curl command (credentials from RANGER_ADMIN_USER / RANGER_ADMIN_PASS):"
  show_ranger_rest_display "${method}" "${url}" "${body_file}"
  say

  local do_run=0
  if [[ "${AUTO_RUN}" == "1" ]]; then
    do_run=1
  elif prompt_run_in_pane; then
    do_run=1
  else
    say "[skipped]"
  fi

  if [[ ${do_run} -eq 1 ]]; then
    say "Response (raw REST — headers + body):"
    say
    exec_ranger_rest "${method}" "${url}" "${body_file}" || true
    say
    local choice
    choice="$(read_choice "Press Enter after reviewing the response (q=quit): ")"
    if [[ "${choice}" == "q" ]]; then
      echo "Demo stopped."
      exit 0
    fi
  fi
}

run_shown() {
  local cmd="$1"
  local prompt="${2:-Press Enter to run, s=skip, q=quit: }"

  if [[ "${ACTIVE_STEP}" != "1" ]]; then
    return 0
  fi

  say
  say "Command:"
  say "  ${cmd}"
  say

  local do_run=0
  if [[ "${AUTO_RUN}" == "1" ]]; then
    do_run=1
  else
    local rc
    prompt_continue "${prompt}"
    rc=$?
    if [[ ${rc} -eq 0 ]]; then
      do_run=1
    elif [[ ${rc} -eq 1 ]]; then
      say "[skipped]"
    fi
  fi

  if [[ ${do_run} -eq 1 ]]; then
    say "Running:"
    say "  ${cmd}"
    say
    eval "${cmd}" || true
  fi
}

run_cmd() {
  run_shown "$1"
}

prompt_run_in_pane() {
  local prompt="${1:-Press Enter to run in this pane (s=skip, q=quit): }"
  local choice
  choice="$(read_choice "${prompt}")"
  case "${choice}" in
    q) echo "Demo stopped."; exit 0 ;;
    s) return 1 ;;
    *) return 0 ;;
  esac
}

run_ranger_curl() {
  if [[ "${ACTIVE_STEP}" != "1" ]]; then
    return 0
  fi

  local url="$1"
  local body_file="$2"

  run_ranger_rest POST "${url}" "${body_file}"
}

manual_step() {
  step_header "$1"
  if [[ "${ACTIVE_STEP}" != "1" ]]; then
    return 0
  fi
  shift
  while (("$#")); do
    say "$1"
    shift
  done
  echo
  local choice
  choice="$(read_choice "Press Enter for next step (q=quit): ")"
  if [[ "${choice}" == "q" ]]; then
    echo "Demo stopped."
    exit 0
  fi
}

write_json() {
  local file="$1"
  cat > "${file}"
}

urlencode_policy_name() {
  local name="$1"
  printf '%s' "${name}" | sed 's/ /%20/g'
}

delete_ranger_policy_by_name() {
  local policy_name="$1"
  local policy_file="/tmp/ranger-delete-${DEMO_RUN_ID}-$(echo "${policy_name}" | tr ' ' '-').json"
  local encoded policy_id

  encoded="$(urlencode_policy_name "${policy_name}")"
  curl -k -u "${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASS}" --fail -H 'accept: application/json' \
    "${RANGER_BASE}/service/public/v2/api/service/cm_ozone/policy/${encoded}" \
    > "${policy_file}" 2>/dev/null || true

  policy_id="$(jq -r .id "${policy_file}" 2>/dev/null || true)"
  if [[ -n "${policy_id}" && "${policy_id}" != "null" ]]; then
    curl -k -u "${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASS}" --fail -X DELETE -H 'accept: application/json' \
      "${RANGER_BASE}/service/public/v2/api/policy/${policy_id}" || true
  fi
  rm -f "${policy_file}"
}

run_demo_cleanup() {
  if [[ "${ACTIVE_STEP}" != "1" ]]; then
    return 0
  fi

  say "Will delete:"
  say "  Ranger policies: ${ASSUME_POLICY_NAME}, ${VOLUME_POLICY_NAME}, ${BUCKET_POLICY_NAME}, ${KEY_POLICY_NAME}"
  say "  Ranger role:     ${SVC_ROLE}"
  say "  Ranger user:     ${SVC_USER}"
  say "  Kerberos principal: ${SVC_KERB_PRINCIPAL}"
  say "  Keytabs:         ${SVC_KEYTAB}, /tmp/${SVC_USER}.keytab"
  say "  Ozone object:    /s3v/${BUCKET}/${DEMO_OBJECT}"
  say "  Ozone bucket:    /s3v/${BUCKET}"
  say "  Local files:     ${LOCAL_DEMO_FILE}, ${DOWNLOAD_FILE}"
  say
  say "Press Enter once to run all cleanup commands (s=skip, q=quit)."

  local do_run=0
  if [[ "${AUTO_RUN}" == "1" ]]; then
    do_run=1
  else
    local rc
    prompt_continue "Press Enter to run cleanup, s=skip, q=quit: "
    rc=$?
    if [[ ${rc} -eq 0 ]]; then
      do_run=1
    elif [[ ${rc} -eq 1 ]]; then
      say "[cleanup skipped]"
      return 0
    fi
  fi

  if [[ ${do_run} -ne 1 ]]; then
    return 0
  fi

  say
  say "Running cleanup..."

  say "  Delete Ranger policy: ${ASSUME_POLICY_NAME}"
  delete_ranger_policy_by_name "${ASSUME_POLICY_NAME}"
  say "  Delete Ranger policy: ${VOLUME_POLICY_NAME}"
  delete_ranger_policy_by_name "${VOLUME_POLICY_NAME}"
  say "  Delete Ranger policy: ${BUCKET_POLICY_NAME}"
  delete_ranger_policy_by_name "${BUCKET_POLICY_NAME}"
  say "  Delete Ranger policy: ${KEY_POLICY_NAME}"
  delete_ranger_policy_by_name "${KEY_POLICY_NAME}"

  say "  Delete Ranger role: ${SVC_ROLE}"
  curl -k -u "${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASS}" --fail -X DELETE -H 'accept: application/json' \
    "${RANGER_BASE}/service/roles/roles/name/${SVC_ROLE}" || true

  say "  Delete Ranger user: ${SVC_USER}"
  curl -k -u "${RANGER_ADMIN_USER}:${RANGER_ADMIN_PASS}" --fail -X DELETE -H 'accept: application/json' \
    "${RANGER_BASE}/service/xusers/users/userName/${SVC_USER}" || true

  say "  Delete Kerberos principal: ${SVC_KERB_PRINCIPAL}"
  kadmin.local -q "delprinc -force ${SVC_KERB_PRINCIPAL}" || true
  rm -f "${SVC_KEYTAB}" "/tmp/${SVC_USER}.keytab"

  say "  Delete Ozone object and bucket"
  kdestroy -A 2>/dev/null || true
  kinit -kt "${HDFS_KEYTAB}" "${HDFS_PRINCIPAL}" || true
  OZONE_OPTS= ozone sh key delete "/s3v/${BUCKET}/${DEMO_OBJECT}" || true
  OZONE_OPTS= ozone sh bucket delete "/s3v/${BUCKET}" || true
  rm -f "${LOCAL_DEMO_FILE}" "${DOWNLOAD_FILE}"
  rm -f "${STATE_FILE}"

  say
  say "Cleanup complete."
}

# ---------------------------------------------------------------------------
# Demo flow
# ---------------------------------------------------------------------------
if (( START_STEP > 1 )); then
  load_demo_state || true
fi

banner "Ozone STS + Ranger AssumeRole — Interactive Demo"
if [[ "${AUTO_RUN}" == "1" ]]; then
  say "AUTO_RUN=1 — running all steps without prompts"
fi
if (( START_STEP > 1 )); then
  say "Resuming from step ${START_STEP} (state: ${STATE_FILE})"
fi
say "Demo run id: ${DEMO_RUN_ID}"
say "Ranger:  ${RANGER_BASE}"
say "Ozone:   ${OZONE_S3G_HOST} (STS: ${OZONE_STS_ENDPOINT})"
say "Service user: ${SVC_USER}"
say "Ranger role:  ${SVC_ROLE}"
say "Assume policy: ${ASSUME_POLICY_NAME}"
echo

manual_step "Turn on STS feature flag for Ozone (Cloudera Manager UI)" \
  "1. In CM search bar, type: site.xml" \
  "2. Open: Ozone Service Advanced Configuration Snippet (Safety Valve)" \
  "     Property: ozone-conf/ozone-site.xml_service_safety_valve" \
  "3. Add property:" \
  "     ozone.s3g.sts.http.enabled = true" \
  "4. Save changes and restart/redeploy Ozone S3 Gateway if prompted."

manual_step "Turn on STS feature flag for Ranger (Cloudera Manager UI)" \
  "1. In CM search bar, type: ranger-admin-site.xml" \
  "2. Open: Admin Advanced Configuration Snippet (Safety Valve)" \
  "     Property: conf/ranger-admin-site.xml_role_safety_valve" \
  "3. Add property:" \
  "     ranger.servicedef.ozone.enableActionMatcherInPoliciesCondition = true" \
  "4. Save changes and restart Ranger Admin if prompted."

step_header "Create service user in Ranger"
say "User: ${SVC_USER}"
say "This user will obtain permanent S3 credentials and assume the Ranger role."
USER_JSON="$(mktemp)"
write_json "${USER_JSON}" <<EOF
{
  "loginId": "${SVC_USER}",
  "name": "${SVC_USER}",
  "password": "${SVC_PASSWORD}",
  "firstName": "Iceberg REST",
  "lastName": "Catalog",
  "emailAddress": "${SVC_USER}@example.com",
  "userRoleList": ["ROLE_USER"],
  "userPermList": [
    { "moduleId": 1, "isAllowed": 1 },
    { "moduleId": 3, "isAllowed": 1 },
    { "moduleId": 7, "isAllowed": 1 }
  ]
}
EOF
run_ranger_curl "${RANGER_BASE}/service/xusers/secure/users" "${USER_JSON}"
rm -f "${USER_JSON}"
save_demo_state

step_header "Create Ranger role"
say "Role: ${SVC_ROLE}"
ROLE_JSON="$(mktemp)"
write_json "${ROLE_JSON}" <<EOF
{
  "name": "${SVC_ROLE}",
  "description": "Iceberg data all access"
}
EOF
run_ranger_curl "${RANGER_BASE}/service/roles/roles" "${ROLE_JSON}"
rm -f "${ROLE_JSON}"
save_demo_state

step_header "Create Ranger policy — ASSUME_ROLE for service user"
say "Policy name: ${ASSUME_POLICY_NAME}"
say "Resource type: Role (not Volume)"
say "User ${SVC_USER} gets assume_role on role ${SVC_ROLE}"
POLICY_JSON="$(mktemp)"
write_json "${POLICY_JSON}" <<EOF
{
  "isEnabled": true,
  "service": "cm_ozone",
  "name": "${ASSUME_POLICY_NAME}",
  "policyType": 0,
  "policyPriority": 0,
  "isAuditEnabled": true,
  "resources": {
    "role": {
      "values": ["${SVC_ROLE}"],
      "isExcludes": false,
      "isRecursive": false
    }
  },
  "policyItems": [
    {
      "accesses": [ { "type": "assume_role", "isAllowed": true } ],
      "users": [ "${SVC_USER}" ],
      "delegateAdmin": false
    }
  ],
  "serviceType": "ozone",
  "isDenyAllElse": false
}
EOF
run_ranger_curl "${RANGER_BASE}/service/public/v2/api/policy" "${POLICY_JSON}"
rm -f "${POLICY_JSON}"
save_demo_state

step_header "Create Ranger data access policies"
say "Create three new Ranger policies for role ${SVC_ROLE}:"
say "  1. Volume — ${VOLUME_POLICY_NAME}"
say "  2. Bucket — ${BUCKET_POLICY_NAME}"
say "  3. Key    — ${KEY_POLICY_NAME}"
say "Each section POSTs a new policy (raw REST response shown)."
say

policy_section_header "VOLUME" "${VOLUME_POLICY_NAME}" "volume=${OZONE_VOLUME}"
VOLUME_POLICY_JSON="$(mktemp)"
write_json "${VOLUME_POLICY_JSON}" <<EOF
{
  "isEnabled": true,
  "service": "cm_ozone",
  "name": "${VOLUME_POLICY_NAME}",
  "policyType": 0,
  "policyPriority": 0,
  "isAuditEnabled": true,
  "resources": {
    "volume": {
      "values": ["${OZONE_VOLUME}"],
      "isExcludes": false,
      "isRecursive": false
    }
  },
  "policyItems": [
    {
      "accesses": [ { "type": "all", "isAllowed": true } ],
      "roles": [ "${SVC_ROLE}" ],
      "delegateAdmin": false
    }
  ],
  "serviceType": "ozone",
  "isDenyAllElse": false
}
EOF
run_ranger_curl "${RANGER_BASE}/service/public/v2/api/policy" "${VOLUME_POLICY_JSON}"
rm -f "${VOLUME_POLICY_JSON}"

policy_section_header "BUCKET" "${BUCKET_POLICY_NAME}" "volume=${OZONE_VOLUME}, bucket=${BUCKET}"
BUCKET_POLICY_JSON="$(mktemp)"
write_json "${BUCKET_POLICY_JSON}" <<EOF
{
  "isEnabled": true,
  "service": "cm_ozone",
  "name": "${BUCKET_POLICY_NAME}",
  "policyType": 0,
  "policyPriority": 0,
  "isAuditEnabled": true,
  "resources": {
    "volume": {
      "values": ["${OZONE_VOLUME}"],
      "isExcludes": false,
      "isRecursive": false
    },
    "bucket": {
      "values": ["${BUCKET}"],
      "isExcludes": false,
      "isRecursive": false
    }
  },
  "policyItems": [
    {
      "accesses": [ { "type": "all", "isAllowed": true } ],
      "roles": [ "${SVC_ROLE}" ],
      "delegateAdmin": false
    }
  ],
  "serviceType": "ozone",
  "isDenyAllElse": false
}
EOF
run_ranger_curl "${RANGER_BASE}/service/public/v2/api/policy" "${BUCKET_POLICY_JSON}"
rm -f "${BUCKET_POLICY_JSON}"

policy_section_header "KEY" "${KEY_POLICY_NAME}" "volume=${OZONE_VOLUME}, bucket=${BUCKET}, key=*"
KEY_POLICY_JSON="$(mktemp)"
write_json "${KEY_POLICY_JSON}" <<EOF
{
  "isEnabled": true,
  "service": "cm_ozone",
  "name": "${KEY_POLICY_NAME}",
  "policyType": 0,
  "policyPriority": 0,
  "isAuditEnabled": true,
  "resources": {
    "volume": {
      "values": ["${OZONE_VOLUME}"],
      "isExcludes": false,
      "isRecursive": false
    },
    "bucket": {
      "values": ["${BUCKET}"],
      "isExcludes": false,
      "isRecursive": false
    },
    "key": {
      "values": ["*"],
      "isExcludes": false,
      "isRecursive": true
    }
  },
  "policyItems": [
    {
      "accesses": [ { "type": "all", "isAllowed": true } ],
      "roles": [ "${SVC_ROLE}" ],
      "delegateAdmin": false
    }
  ],
  "serviceType": "ozone",
  "isDenyAllElse": false
}
EOF
run_ranger_curl "${RANGER_BASE}/service/public/v2/api/policy" "${KEY_POLICY_JSON}"
rm -f "${KEY_POLICY_JSON}"
save_demo_state

step_header "Authenticate as hdfs and create demo bucket/object in Ozone"

say "kinit as the hdfs service principal for this host:"
say
say "  kinit -kt ${HDFS_KEYTAB} hdfs/<_HOST>@${KERB_REALM}"
say
say "On this host: _HOST=${HDFS_HOST}"
run_cmd "kdestroy -A || true"
run_cmd "kinit -kt ${HDFS_KEYTAB} ${HDFS_PRINCIPAL}"

say "Issue this command to create the demo bucket:"
run_cmd "ozone sh bucket create /s3v/${BUCKET}"

say "Issue this command to create a temporary file:"
run_cmd "echo \"string for demo\" > ${LOCAL_DEMO_FILE}"

say "Issue this command to upload this temporary file into the demo bucket:"
run_cmd "ozone sh key put /s3v/${BUCKET}/${DEMO_OBJECT} ${LOCAL_DEMO_FILE}"

say "Install AWS CLI if needed:"
run_cmd "pip install awscli"

say "Verify AWS CLI version:"
run_cmd "aws --version"

step_header "Create ${SVC_USER} Kerberos principal and keytab"
say "Principal: ${SVC_KERB_PRINCIPAL}"
say "Keytab:    ${SVC_KEYTAB}"
say "Delete any leftover principal/keytab from a prior run, then create fresh credentials."
run_cmd "kadmin.local -q \"delprinc -force ${SVC_KERB_PRINCIPAL}\" || true"
run_cmd "rm -f /tmp/${SVC_USER}.keytab ${SVC_KEYTAB}"
run_cmd "kadmin.local -q \"addprinc -randkey ${SVC_KERB_PRINCIPAL}\""
run_cmd "kadmin.local -q \"xst -k /tmp/${SVC_USER}.keytab ${SVC_KERB_PRINCIPAL}\""
run_cmd "cp /tmp/${SVC_USER}.keytab ${SVC_KEYTAB}"

step_header "Get permanent S3 credentials for ${SVC_USER}"
say "Authenticate with Kerberos, then run getsecret -e (prints export statements):"
say "  OZONE_OPTS= ozone s3 getsecret -e"
run_cmd "kdestroy -A || true"
run_cmd "kinit -kt ${SVC_KEYTAB} ${SVC_KERB_PRINCIPAL}"
run_cmd "OZONE_OPTS= ozone s3 getsecret -e"

say "Export credentials into this shell:"
run_cmd "eval \"\$(OZONE_OPTS= ozone s3 getsecret -e)\""
run_cmd "echo \"AWS_ACCESS_KEY_ID=\${AWS_ACCESS_KEY_ID}\""

step_header "List bucket objects with permanent credentials - expected success"
say "Endpoint: ${OZONE_S3G_ENDPOINT}"
say "Permanent creds from ozone s3 getsecret can list the bucket."
run_cmd "aws s3api list-objects --bucket ${BUCKET} --endpoint-url ${OZONE_S3G_ENDPOINT} --no-verify-ssl"

step_header "Get object with permanent credentials - expected failure"
say "Endpoint: ${OZONE_S3G_ENDPOINT}"
say "Permanent creds alone cannot read objects — this should fail until STS session creds are used."
say "Demo command (expected to fail):"
say "  aws s3api get-object --bucket ${BUCKET} --key ${DEMO_OBJECT} ${DOWNLOAD_FILE} --endpoint-url ${OZONE_S3G_ENDPOINT} --no-verify-ssl"
run_cmd "aws s3api get-object --bucket ${BUCKET} --key ${DEMO_OBJECT} ${DOWNLOAD_FILE} --endpoint-url ${OZONE_S3G_ENDPOINT} --no-verify-ssl || true"

step_header "Call AssumeRole API — obtain limited-scope STS credentials"
say "Notes:"
say "  - Account id 123456789012 in role-arn is dummy (valid length only)"
say "  - Role suffix must match Ranger role: ${SVC_ROLE}"
say "  - role-session-name is dummy but must meet AWS naming rules"
say "  - Inline policy limits access to s3:GetObject on ${BUCKET}/*"
ASSUME_ROLE_CMD="aws sts assume-role --role-arn arn:aws:iam::123456789012:role/${SVC_ROLE} --role-session-name sess-${DEMO_RUN_ID} --policy '{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"s3:GetObject\",\"Resource\":\"arn:aws:s3:::${BUCKET}/*\"}]}' --duration-seconds 3600 --endpoint-url ${OZONE_STS_ENDPOINT} --no-verify-ssl"
run_cmd "${ASSUME_ROLE_CMD}"

say "Export STS session credentials into this shell:"
run_cmd "CREDS=\$(${ASSUME_ROLE_CMD}) && export AWS_ACCESS_KEY_ID=\$(echo \"\$CREDS\" | jq -r .Credentials.AccessKeyId) && export AWS_SECRET_ACCESS_KEY=\$(echo \"\$CREDS\" | jq -r .Credentials.SecretAccessKey) && export AWS_SESSION_TOKEN=\$(echo \"\$CREDS\" | jq -r .Credentials.SessionToken)"
run_cmd "echo \"export AWS_ACCESS_KEY_ID='\${AWS_ACCESS_KEY_ID}'\" && echo \"export AWS_SECRET_ACCESS_KEY='\${AWS_SECRET_ACCESS_KEY}'\" && echo \"export AWS_SESSION_TOKEN='\${AWS_SESSION_TOKEN}'\""

step_header "Download demo object using STS credentials - expected success"
say "Endpoint: ${OZONE_S3G_ENDPOINT}"
say "STS session creds are exported — get-object should succeed."
run_cmd "aws s3api get-object --bucket ${BUCKET} --key ${DEMO_OBJECT} ${DOWNLOAD_FILE} --endpoint-url ${OZONE_S3G_ENDPOINT} --no-verify-ssl"

step_header "Verify downloaded content"
run_cmd "cat ${DOWNLOAD_FILE}"

step_header "Cleanup demo resources"
run_demo_cleanup

banner "Demo complete"
say "Summary:"
say "  1. Enabled STS in Ozone and Ranger"
say "  2. Created Ranger user, role, and assume_role policy"
say "  3. Created volume/bucket/key data access policies for ${SVC_ROLE}"
say "  4. Created Ozone bucket/object and Kerberos principal"
say "  5. Obtained permanent S3 creds via ozone s3 getsecret -e"
say "  6. list-objects succeeded; get-object failed with permanent creds only"
say "  7. Assumed role via STS for limited-scope session creds"
say "  8. get-object succeeded with STS session credentials"
say "  9. Cleaned up demo user, role, policies, keytabs, and Ozone objects"
