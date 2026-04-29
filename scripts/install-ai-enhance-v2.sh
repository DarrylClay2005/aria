#!/usr/bin/env bash
set -Eeuo pipefail
APP_DIR="$HOME/.local/share/ai-enhance"
BIN_DIR="$HOME/.local/bin"
SERVICE_DIR="$HOME/.local/share/kio/servicemenus"
BINARY="$APP_DIR/realesrgan-ncnn-vulkan"
ZIP="$APP_DIR/realesrgan.zip"
URL="https://github.com/xinntao/Real-ESRGAN/releases/download/v0.2.5.0/realesrgan-ncnn-vulkan-20220424-ubuntu.zip"
log(){ printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
fail(){ printf '\n\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }
log "Installing AI Enhance for KDE/ZorinOS"
sudo apt update
sudo apt install -y wget curl unzip libvulkan1 libgomp1 mesa-vulkan-drivers imagemagick konsole || sudo apt install -y wget curl unzip libvulkan1 libgomp1 mesa-vulkan-drivers imagemagick
mkdir -p "$APP_DIR" "$BIN_DIR" "$SERVICE_DIR"
cd "$APP_DIR"
if [[ ! -x "$BINARY" ]]; then
  log "Downloading Real-ESRGAN Vulkan build"
  rm -f "$ZIP"
  wget -O "$ZIP" "$URL"
  log "Extracting"
  rm -rf "$APP_DIR/extract"
  mkdir -p "$APP_DIR/extract"
  unzip -o "$ZIP" -d "$APP_DIR/extract"
  FOUND="$(find "$APP_DIR/extract" -type f -name 'realesrgan-ncnn-vulkan' -print -quit || true)"
  [[ -n "$FOUND" ]] || fail "Could not find realesrgan-ncnn-vulkan inside the downloaded zip."
  cp "$FOUND" "$BINARY"
  chmod +x "$BINARY"
  FOUND_MODELS="$(find "$APP_DIR/extract" -type d -name models -print -quit || true)"
  [[ -n "$FOUND_MODELS" ]] || fail "Could not find models folder inside the downloaded zip."
  rm -rf "$APP_DIR/models"
  cp -a "$FOUND_MODELS" "$APP_DIR/models"
  rm -rf "$APP_DIR/extract"
else
  log "Real-ESRGAN already installed"
fi
log "Creating ai-enhance command"
cat > "$BIN_DIR/ai-enhance" <<'SCRIPT'
#!/usr/bin/env bash
set -Eeuo pipefail
APP_DIR="$HOME/.local/share/ai-enhance"
BINARY="$APP_DIR/realesrgan-ncnn-vulkan"
MODELS="$APP_DIR/models"
MODEL="${AI_ENHANCE_MODEL:-realesrgan-x4plus}"
SCALE="${AI_ENHANCE_SCALE:-4}"
TILE="${AI_ENHANCE_TILE:-256}"
[[ $# -gt 0 ]] || { echo "Usage: ai-enhance IMAGE_OR_FOLDER [more images/folders...]"; exit 1; }
[[ -x "$BINARY" ]] || { echo "Real-ESRGAN binary missing: $BINARY" >&2; exit 1; }
[[ -d "$MODELS" ]] || { echo "Real-ESRGAN models missing: $MODELS" >&2; exit 1; }
process_file(){
  local input="$1"
  [[ -f "$input" ]] || { echo "Skipping non-file: $input"; return 0; }
  case "${input,,}" in *.png|*.jpg|*.jpeg|*.webp|*.bmp) ;; *) echo "Skipping unsupported file: $input"; return 0 ;; esac
  local dir base name out
  dir="$(dirname "$input")"; base="$(basename "$input")"; name="${base%.*}"; out="$dir/${name}_AI_ENHANCED.png"
  echo "Enhancing: $input"
  "$BINARY" -i "$input" -o "$out" -n "$MODEL" -s "$SCALE" -t "$TILE" -m "$MODELS"
  echo "Saved: $out"
}
for target in "$@"; do
  if [[ -d "$target" ]]; then
    find "$target" -maxdepth 1 -type f \( -iname '*.png' -o -iname '*.jpg' -o -iname '*.jpeg' -o -iname '*.webp' -o -iname '*.bmp' \) -print0 | while IFS= read -r -d '' file; do process_file "$file"; done
  else
    process_file "$target"
  fi
done
SCRIPT
chmod +x "$BIN_DIR/ai-enhance"
if ! grep -q 'export PATH="$HOME/.local/bin:$PATH"' "$HOME/.bashrc" 2>/dev/null; then echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$HOME/.bashrc"; fi
export PATH="$BIN_DIR:$PATH"
log "Creating Dolphin right-click menu"
cat > "$SERVICE_DIR/ai-enhance.desktop" <<'DESKTOP'
[Desktop Entry]
Type=Service
MimeType=image/png;image/jpeg;image/webp;image/bmp;
Actions=AIEnhance;AIEnhanceAnime;
X-KDE-ServiceTypes=KonqPopupMenu/Plugin
Icon=image
[Desktop Action AIEnhance]
Name=AI Enhance 4x
Icon=image
Exec=konsole --hold -e bash -lc 'source ~/.bashrc; ai-enhance "$@"' ai-enhance %F
[Desktop Action AIEnhanceAnime]
Name=AI Enhance Anime 4x
Icon=image
Exec=konsole --hold -e bash -lc 'source ~/.bashrc; AI_ENHANCE_MODEL=realesrgan-x4plus-anime ai-enhance "$@"' ai-enhance %F
DESKTOP
command -v kbuildsycoca5 >/dev/null 2>&1 && kbuildsycoca5 >/dev/null 2>&1 || true
command -v kbuildsycoca6 >/dev/null 2>&1 && kbuildsycoca6 >/dev/null 2>&1 || true
log "Testing Real-ESRGAN binary"
"$BINARY" -h >/dev/null || fail "Real-ESRGAN exists but could not start. Vulkan/GPU driver may be missing."
printf '\nDONE. Use: ai-enhance /path/to/image.png\n'
