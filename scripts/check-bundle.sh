#!/bin/sh
# Checks the Maven Central bundle of a release before anything is published:
#
#   ./gradlew nmcpZipAggregation -Pversion=0.6.0
#   scripts/check-bundle.sh build/nmcp/zip/aggregation.zip 0.6.0
#
# $3 is the repository root (default: the current directory). The bundle passes when:
# - it holds exactly the modules whose build applies `maven-publish`, each at this version only;
# - each module has its jar, sources jar, javadoc jar, POM and Gradle metadata;
# - every com.damoreira.snodra coordinate in a POM or metadata file carries this version
#   (an add-on never points at another core);
# - the core jar reports this version (META-INF/snodra-messaging/version.properties).
# Signatures are not checked: the release adds them.
set -eu

group=com.damoreira.snodra
core=snodra-messaging

usage() {
  echo "usage: $0 BUNDLE.zip X.Y.Z [ROOT]" >&2
  exit 2
}

[ $# -ge 2 ] || usage
zip=$1 version=$2 root=${3:-.}
grep -Eqx '(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)' <<EOF || usage
$version
EOF
[ -f "$zip" ] || { echo "check-bundle: no bundle at $zip" >&2; exit 1; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
errors=0
bad() {
  echo "check-bundle: $*" >&2
  errors=$((errors + 1))
}

# The modules the build publishes.
modules=
for f in "$root"/*/build.gradle.kts; do
  grep -q 'maven-publish' "$f" && modules="$modules $(basename "$(dirname "$f")")"
done
[ -n "$modules" ] || { echo "check-bundle: no module under $root applies maven-publish" >&2; exit 1; }

gpath=$(echo "$group" | tr . /)
unzip -Z1 "$zip" >"$work/entries"

# Every file sits at <group>/<published module>/<version>/.
while IFS= read -r e; do
  case $e in */) continue ;; esac
  rest=${e#"$gpath"/}
  if [ "$rest" = "$e" ]; then
    bad "$e is outside $group"
    continue
  fi
  m=${rest%%/*} rest=${rest#*/}
  v=${rest%%/*}
  case " $modules " in *" $m "*) ;; *) bad "$m is in the bundle but the build does not publish it" ;; esac
  [ "$v" = "$version" ] || bad "$e is not at version $version"
done <"$work/entries"

for m in $modules; do
  d=$gpath/$m/$version
  for f in "$m-$version.jar" "$m-$version-sources.jar" "$m-$version-javadoc.jar" "$m-$version.pom" "$m-$version.module"; do
    grep -qxF "$d/$f" "$work/entries" || { bad "$d/$f is missing"; continue; }
    case $f in *.pom | *.module) ;; *) continue ;; esac
    # The first version after each line naming the group is that coordinate's version.
    unzip -p "$zip" "$d/$f" >"$work/meta"
    awk -v v="$version" -v g="$group" -v f="$f" '
      index($0, g) { want = 1; next }
      want && match($0, /[0-9]+\.[0-9]+\.[0-9]+[^"<]*/) {
        got = substr($0, RSTART, RLENGTH); want = 0; n++
        if (got != v) { print "check-bundle: " f " names " g " at " got >"/dev/stderr"; bad = 1 }
      }
      END { if (!n) { print "check-bundle: " f " names no " g " version" >"/dev/stderr"; bad = 1 }; exit bad }
    ' "$work/meta" || errors=$((errors + 1))
  done
done

# The version the core sends to the hub in Register.
case " $modules " in
  *" $core "*)
    jar=$gpath/$core/$version/$core-$version.jar
    if unzip -p "$zip" "$jar" >"$work/core.jar" 2>/dev/null &&
      reported=$(unzip -p "$work/core.jar" META-INF/snodra-messaging/version.properties 2>/dev/null); then
      [ "$reported" = "version=$version" ] || bad "$jar reports '$reported', not version=$version"
    else
      bad "$jar has no META-INF/snodra-messaging/version.properties"
    fi
    ;;
esac

if [ "$errors" -ne 0 ]; then
  echo "check-bundle: $errors problem(s) in $zip" >&2
  exit 1
fi
echo "check-bundle: $zip holds$modules at $version"
