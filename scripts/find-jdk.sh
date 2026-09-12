#!/usr/bin/env bash
# Imprime um JAVA_HOME com JDK 21 ou superior (exigência do Spring Boot 4).
#
# Existe porque é comum a máquina ter um JDK antigo no PATH e um recente
# instalado ao lado — o erro nesse caso ("release version 21 not supported")
# aponta para o compilador, não para o JAVA_HOME, e custa tempo.
set -euo pipefail

version_of() {
  local home="$1"
  [ -x "$home/bin/javac" ] || return 1
  "$home/bin/javac" -version 2>&1 | sed -E 's/^javac ([0-9]+).*/\1/'
}

candidates=()
[ -n "${JAVA_HOME:-}" ] && candidates+=("$JAVA_HOME")
command -v javac >/dev/null 2>&1 && candidates+=("$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")")
for dir in /usr/lib/jvm/* "$HOME"/.sdkman/candidates/java/*; do
  [ -d "$dir" ] && candidates+=("$dir")
done

best_home=""
best_version=0
for home in "${candidates[@]}"; do
  version="$(version_of "$home" 2>/dev/null || true)"
  [[ "$version" =~ ^[0-9]+$ ]] || continue
  if [ "$version" -ge 21 ] && [ "$version" -gt "$best_version" ]; then
    best_version="$version"
    best_home="$home"
  fi
done

if [ -z "$best_home" ]; then
  echo "nenhum JDK 21+ encontrado; instale um ou passe make JAVA_HOME=/caminho" >&2
  exit 1
fi
echo "$best_home"
