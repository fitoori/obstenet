#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() {
  printf "[%s] %s\n" "$(date +"%Y-%m-%d %H:%M:%S")" "$*"
}

remounted_boot=""
remount_boot_rw() {
  local mountpoints=("/boot/firmware" "/boot")
  local mp
  for mp in "${mountpoints[@]}"; do
    if mountpoint -q "$mp"; then
      if mount | grep -E "on ${mp} " | grep -q "\(.*ro[ ,)]"; then
        log "Remounting ${mp} read-write"
        sudo mount -o remount,rw "$mp"
        remounted_boot="$mp"
        break
      fi
    fi
  done
}

restore_boot_mount() {
  if [[ -n "$remounted_boot" ]]; then
    log "Restoring ${remounted_boot} to read-only"
    sudo mount -o remount,ro "$remounted_boot"
  fi
}

trap restore_boot_mount EXIT

log "Starting update"
remount_boot_rw

log "Updating apt package lists"
sudo apt update

log "Upgrading apt packages"
sudo apt upgrade -y

log "Running apt autoremove"
sudo apt autoremove -y

if ! dpkg -C >/dev/null 2>&1; then
  log "dpkg reports incomplete configuration; running dpkg --configure -a"
  sudo dpkg --configure -a
fi

log "Syncing git repository"
cd "$repo_dir"
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  if [[ -z "$(git status --porcelain)" ]]; then
    git fetch --all --prune
    git pull --rebase
  else
    log "Working tree has local changes; skipping git pull"
  fi
else
  log "Not a git repository; skipping git sync"
fi

update_venv() {
  local venv_path="$1"
  local python_bin="$venv_path/bin/python"
  if [[ -x "$python_bin" ]]; then
    log "Updating pip in ${venv_path}"
    "$python_bin" -m pip install --upgrade pip
    if [[ -f "$repo_dir/requirements.txt" ]]; then
      log "Installing requirements.txt in ${venv_path}"
      "$python_bin" -m pip install -r "$repo_dir/requirements.txt"
    fi
  fi
}

log "Updating Python packages"
venv_candidates=()
if [[ -d "$repo_dir/.venv" ]]; then
  venv_candidates+=("$repo_dir/.venv")
fi
if [[ -d "$repo_dir/venv" ]]; then
  venv_candidates+=("$repo_dir/venv")
fi
if [[ -d "$repo_dir/env" ]]; then
  venv_candidates+=("$repo_dir/env")
fi

if [[ ${#venv_candidates[@]} -gt 0 ]]; then
  for venv_path in "${venv_candidates[@]}"; do
    update_venv "$venv_path"
  done
else
  log "No virtual environments found; updating system pip"
  python3 -m pip install --upgrade pip
  if [[ -f "$repo_dir/requirements.txt" ]]; then
    python3 -m pip install -r "$repo_dir/requirements.txt"
  fi
fi

log "Update complete"
