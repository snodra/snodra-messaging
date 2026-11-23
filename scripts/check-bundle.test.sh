#!/bin/sh
# Tests for check-bundle.sh on small fake bundles: sh scripts/check-bundle.test.sh (needs zip and unzip)
set -u
script=$(cd "$(dirname "$0")" && pwd)/check-bundle.sh
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
failed=0
g=com/damoreira/snodra

# A repository root: two published modules and an example that is not published.
root=$work/repo
for m in snodra-messaging snodra-messaging-kafka examples; do mkdir -p "$root/$m"; done
printf 'plugins {\n    `maven-publish`\n}\n' >"$root/snodra-messaging/build.gradle.kts"
printf 'plugins {\n    `maven-publish`\n}\n' >"$root/snodra-messaging-kafka/build.gradle.kts"
printf 'plugins {\n    application\n}\n' >"$root/examples/build.gradle.kts"

# module DIR NAME VERSION [CORE-VERSION]: the five files of one module, as the build writes them.
module() {
  d=$1/$g/$2/$3
  mkdir -p "$d"
  for c in '' -sources -javadoc; do echo jar >"$d/$2-$3$c.jar"; done
  cat >"$d/$2-$3.pom" <<EOF
<project>
  <groupId>com.damoreira.snodra</groupId>
  <artifactId>$2</artifactId>
  <version>$3</version>
  <dependencies>
    <dependency>
      <groupId>com.damoreira.snodra</groupId>
      <artifactId>snodra-messaging</artifactId>
      <version>${4:-$3}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-clients</artifactId>
      <version>4.2.2</version>
    </dependency>
  </dependencies>
</project>
EOF
  cat >"$d/$2-$3.module" <<EOF
{
  "component": {
    "group": "com.damoreira.snodra",
    "module": "$2",
    "version": "$3"
  },
  "variants": [
    {
      "dependencies": [
        {
          "group": "com.damoreira.snodra",
          "module": "snodra-messaging",
          "version": {
            "requires": "${4:-$3}"
          }
        }
      ]
    }
  ]
}
EOF
}

# core-jar DIR VERSION REPORTED: the core jar carries the version it reports to the hub.
core_jar() {
  j=$work/jar
  rm -rf "$j" && mkdir -p "$j/META-INF/snodra-messaging"
  echo "version=$3" >"$j/META-INF/snodra-messaging/version.properties"
  rm -f "$1/$g/snodra-messaging/$2/snodra-messaging-$2.jar"
  (cd "$j" && zip -qr "$1/$g/snodra-messaging/$2/snodra-messaging-$2.jar" META-INF)
}

# good DIR: a complete 1.2.3 bundle.
good() {
  rm -rf "$1" && mkdir -p "$1"
  module "$1" snodra-messaging 1.2.3
  module "$1" snodra-messaging-kafka 1.2.3
  core_jar "$1" 1.2.3 1.2.3
}

# check NAME EXPECTED-EXIT DIR VERSION: zips DIR and runs the check on it.
check() {
  name=$1 want=$2 dir=$3 version=$4
  rm -f "$work/bundle.zip"
  (cd "$dir" && zip -qr "$work/bundle.zip" .)
  out=$(sh "$script" "$work/bundle.zip" "$version" "$root" 2>&1)
  got=$?
  if [ "$got" -eq "$want" ]; then
    echo "ok   $name"
  else
    echo "FAIL $name: want exit $want, got $got: $out"
    failed=1
  fi
}

b=$work/b

good "$b"
check "a complete bundle passes" 0 "$b" 1.2.3

good "$b"
check "another version fails" 1 "$b" 1.2.4

good "$b"
rm -rf "${b:?}/$g/snodra-messaging-kafka"
check "a missing module fails" 1 "$b" 1.2.3

good "$b"
module "$b" snodra-messaging-extra 1.2.3
check "a module the build does not publish fails" 1 "$b" 1.2.3

good "$b"
module "$b" snodra-messaging-kafka 1.2.2
check "a module also at another version fails" 1 "$b" 1.2.3

good "$b"
rm "$b/$g/snodra-messaging-kafka/1.2.3/snodra-messaging-kafka-1.2.3-sources.jar"
check "a missing sources jar fails" 1 "$b" 1.2.3

good "$b"
rm "$b/$g/snodra-messaging/1.2.3/snodra-messaging-1.2.3.module"
check "missing Gradle metadata fails" 1 "$b" 1.2.3

good "$b"
module "$b" snodra-messaging-kafka 1.2.3 1.2.2
check "an add-on on a stale core fails" 1 "$b" 1.2.3

good "$b"
module "$b" snodra-messaging-kafka 1.2.3 1.2.3-SNAPSHOT
check "an add-on on a snapshot core fails" 1 "$b" 1.2.3

good "$b"
core_jar "$b" 1.2.3 0.1.0
check "a core reporting another version fails" 1 "$b" 1.2.3

good "$b"
echo x >"$b/stray.txt"
check "a file outside the group fails" 1 "$b" 1.2.3

good "$b"
check "a malformed version is a usage error" 2 "$b" 1.2

good "$b"
check "a leading zero is a usage error" 2 "$b" 01.2.3

if sh "$script" "$work/none.zip" 1.2.3 "$root" >/dev/null 2>&1; then
  echo "FAIL a missing bundle fails"
  failed=1
else
  echo "ok   a missing bundle fails"
fi

exit $failed
