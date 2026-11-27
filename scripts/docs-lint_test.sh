#!/usr/bin/env bash
set -euo pipefail
script=$(cd "$(dirname "$0")" && pwd)/docs-lint.sh
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cd "$tmp"
git init -q
git config user.name Test
git config user.email test@example.invalid
git config commit.gpgsign false
git config core.hooksPath /dev/null
mkdir doc
printf 'baseline\n' > baseline
cat > README.md <<'DOC'
---
type: Library
owner: team
status: active
stale_after: 2999-01-01T00:00:00Z
---
# Architecture
DOC
git add baseline README.md
git commit -qm baseline
base=$(git rev-parse HEAD)
make_doc() {
  cat > "doc/$1.md" <<DOC
---
type: guide
owner: team
status: active
stale_after: $2
---
# Test
$3
DOC
}
make_doc good 2999-01-01T00:00:00Z '[baseline](../baseline)'
"$script" --base "$base" --strict >out 2>&1 || { cat out; echo 'FAIL good doc'; exit 1; }
echo 'ok   valid changed doc'
printf 'file\n' > 'doc/a file.txt'
make_doc good 2999-01-01T00:00:00Z '[file](a%20file.txt)'
"$script" --base "$base" --strict >out 2>&1 || { cat out; echo 'FAIL encoded link'; exit 1; }
echo 'ok   encoded relative link'
rm 'doc/a file.txt'
make_doc good 2999-01-01T00:00:00Z '[baseline](../baseline)'

make_doc missing 2999-01-01T00:00:00Z ''
sed -i.bak '/^owner:/d' doc/missing.md
rm doc/missing.md.bak
if "$script" --base "$base" --strict >out 2>&1; then echo 'FAIL missing field accepted'; exit 1; fi
grep -q 'owner' out || { cat out; exit 1; }
echo 'ok   missing field rejected'
make_doc missing 2999-01-01T00:00:00Z ''
sed -i.bak 's/^owner: team$/owner: ""/' doc/missing.md
rm doc/missing.md.bak
if "$script" --base "$base" --strict >out 2>&1; then echo 'FAIL empty field accepted'; exit 1; fi
grep -q 'owner' out || { cat out; exit 1; }
echo 'ok   empty field rejected'
rm doc/missing.md
make_doc broken 2999-01-01T00:00:00Z '[bad](missing.md)'
if "$script" --base "$base" --strict >out 2>&1; then echo 'FAIL broken link accepted'; exit 1; fi
grep -q 'missing.md' out || { cat out; exit 1; }
"$script" --base "$base" --warn-only >out 2>&1 || { cat out; echo 'FAIL warning mode'; exit 1; }
grep -i -q warning out || { cat out; exit 1; }
echo 'ok   broken link rejected; warning mode succeeds'
rm doc/broken.md
make_doc stale 2000-01-01T00:00:00Z ''
"$script" --base "$base" --strict >out 2>&1 || { cat out; echo 'FAIL stale doc'; exit 1; }
grep -i -q 'warning.*stale' out || { cat out; exit 1; }
echo 'ok   stale date warns without failure'
rm doc/stale.md
git add doc/good.md
git commit -qm 'add good document'
base=$(git rev-parse HEAD)
printf 'invalid old doc\n' > doc/old.md
git add doc/old.md
git commit -qm 'add legacy document'
base=$(git rev-parse HEAD)
printf '\nNew prose.\n' >> doc/good.md
"$script" --base "$base" --strict >out 2>&1 || { cat out; echo 'FAIL unchanged doc checked'; exit 1; }
echo 'ok   unchanged legacy doc ignored'
printf '\n[bad](missing.md)\n' >> doc/good.md
git add doc/good.md
if "$script" --base "$base" --strict >out 2>&1; then echo 'FAIL staged broken link accepted'; exit 1; fi
grep -q 'missing.md' out || { cat out; exit 1; }
echo 'ok   staged changed doc checked'
git reset -q HEAD -- doc/good.md
git checkout -- doc/good.md
printf '\n[ref]: absent.md\n' >> doc/good.md
if "$script" --base "$base" --strict >out 2>&1; then echo 'FAIL broken reference link accepted'; exit 1; fi
grep -q 'absent.md' out || { cat out; exit 1; }
echo 'ok   broken reference link rejected'
git checkout -- doc/good.md
cat >> doc/good.md <<'DOC'
````
```
````
[bad](missing.md)
DOC
if "$script" --base "$base" --strict >out 2>&1; then echo 'FAIL mismatched fence concealed broken link'; exit 1; fi
grep -q 'missing.md' out || { cat out; exit 1; }
echo 'ok   fence length'
git checkout -- doc/good.md
cat > README.md <<'DOC'
---
type: Library
owner: team
status: active
stale_after: 2999-01-01T00:00:00Z
---
# Architecture
[bad](missing.md)
DOC
if "$script" --base "$base" --strict >out 2>&1; then echo 'FAIL root doc skipped'; exit 1; fi
grep -q 'missing.md' out || { cat out; exit 1; }
echo 'ok   root doc checked'
git checkout -- README.md
git checkout -- doc/good.md
mkdir -p 'doc/product specs'
make_doc 'product specs/a b' 2999-01-01T00:00:00Z ''
printf '\n[spec](<product specs/a b.md>)\n' >> doc/good.md
"$script" --base "$base" --strict >out 2>&1 || { cat out; echo 'FAIL angle destination'; exit 1; }
echo 'ok   angle destination with spaces'
git checkout -- doc/good.md
make_doc 'notes_(v2)' 2999-01-01T00:00:00Z ''
printf '\n[notes](notes_(v2).md)\n' >> doc/good.md
"$script" --base "$base" --strict >out 2>&1 || { cat out; echo 'FAIL parenthesized destination'; exit 1; }
echo 'ok   parenthesized destination'
git checkout -- doc/good.md
printf '# No frontmatter\n[bad](missing.md)\n' > doc/plain.md
if "$script" --base "$base" --strict >out 2>&1; then echo 'FAIL invalid doc accepted'; exit 1; fi
if ! grep -q 'missing frontmatter' out || ! grep -q 'broken link missing.md' out; then cat out; echo 'FAIL missing-frontmatter link not checked'; exit 1; fi
echo 'ok   links checked without frontmatter'
rm doc/plain.md
cat > doc/block.md <<'DOC'
---
type: guide
owner:
  - "@x"
status: active
stale_after: 2999-01-01T00:00:00Z
---
# Test
DOC
"$script" --base "$base" --strict >out 2>&1 || { cat out; echo 'FAIL block owner'; exit 1; }
echo 'ok   block owner accepted'
rm doc/block.md
for field in owner status; do
  make_doc comments 2999-01-01T00:00:00Z ''
  if [ "$field" = owner ]; then sed -i.bak 's/^owner: team$/owner: "" # TODO/' doc/comments.md; else sed -i.bak 's/^status: active$/status: # pending/' doc/comments.md; fi
  rm doc/comments.md.bak
  if "$script" --base "$base" --strict >out 2>&1; then echo "FAIL commented empty $field accepted"; exit 1; fi
  grep -q "missing $field" out || { cat out; exit 1; }
done
make_doc comments 2999-01-01T00:00:00Z ''
sed -i.bak 's/^type: guide$/type: reference # note/; s/^owner: team$/owner: "team # name" # note/; s/^stale_after: .*/stale_after: 2999-01-01T00:00:00Z # note/' doc/comments.md
rm doc/comments.md.bak
"$script" --base "$base" --strict >out 2>&1 || { cat out; echo 'FAIL valid trailing comment'; exit 1; }
echo 'ok   YAML comments'
rm doc/comments.md
make_doc refs 2999-01-01T00:00:00Z '[guide][missing] and [absent][]'
if "$script" --base "$base" --strict >out 2>&1; then echo 'FAIL undefined references accepted'; exit 1; fi
if ! grep -qi 'missing' out || ! grep -qi 'absent' out; then cat out; echo 'FAIL missing labels'; exit 1; fi
make_doc refs 2999-01-01T00:00:00Z '[guide][  MIXED   Case ] and [mixed case][] and [ordinary] and [ ]'
printf '\n[mixed  CASE]: ../baseline\n' >> doc/refs.md
"$script" --base "$base" --strict >out 2>&1 || { cat out; echo 'FAIL defined reference'; exit 1; }
make_doc refs 2999-01-01T00:00:00Z '[guide][defined]'
printf '\n[defined]: nonexistent.md\n' >> doc/refs.md
if "$script" --base "$base" --strict >out 2>&1; then echo 'FAIL broken reference target accepted'; exit 1; fi
grep -q nonexistent.md out || { cat out; exit 1; }
echo 'ok   reference labels and targets'
rm doc/refs.md
make_doc indented 2999-01-01T00:00:00Z ''
printf '    ```\n[bad](missing.md)\n' >> doc/indented.md
if "$script" --base "$base" --strict >out 2>&1; then echo 'FAIL indented marker hid link'; exit 1; fi
grep -q missing.md out || { cat out; exit 1; }
make_doc indented 2999-01-01T00:00:00Z ''
# shellcheck disable=SC2016
printf '```\n    ```\n[ignored](missing.md)\n```\n[good](../baseline)\n' >> doc/indented.md
"$script" --base "$base" --strict >out 2>&1 || { cat out; echo 'FAIL indented marker closed fence'; exit 1; }
echo 'ok   indented fence markers'
make_doc indented 2999-01-01T00:00:00Z ''
printf '    ~~~\n[bad](missing.md)\n' >> doc/indented.md
if "$script" --base "$base" --strict >out 2>&1; then echo 'FAIL indented tilde marker hid link'; exit 1; fi
grep -q missing.md out || { cat out; exit 1; }
make_doc indented 2999-01-01T00:00:00Z ''
printf '~~~\n    ~~~\n[ignored](missing.md)\n~~~\n[good](../baseline)\n' >> doc/indented.md
"$script" --base "$base" --strict >out 2>&1 || { cat out; echo 'FAIL indented tilde marker closed fence'; exit 1; }
echo 'ok   indented tilde markers'
rm doc/indented.md

# Legacy and root documents do not require frontmatter.
git checkout -- doc/good.md
printf 'legacy\n' > doc/legacy.md
git add doc/legacy.md && git commit -qm legacy
base=$(git rev-parse HEAD)
printf 'changed\n' >> doc/legacy.md
printf '# Root without metadata\n' > README.md
"$script" --base "$base" --strict >out 2>&1 || { cat out; echo 'FAIL legacy and root metadata'; exit 1; }
echo 'ok   existing and root docs need no frontmatter'
mkdir -p doc/nested
printf '# new nested\n' > doc/nested/new.md
if "$script" --base "$base" --strict >out 2>&1; then echo 'FAIL new nested metadata'; exit 1; fi
grep -q 'missing frontmatter' out || { cat out; exit 1; }
echo 'ok   new nested doc requires frontmatter'
rm doc/nested/new.md
printf '# ignored\n[bad](missing.md)\n' > CHANGELOG.md
mkdir -p .changes && printf '[bad](missing.md)\n' > .changes/ignored.md
"$script" --base "$base" --strict >out 2>&1 || { cat out; echo 'FAIL excluded files checked'; exit 1; }
echo 'ok   changelog and changes excluded'
make_doc date-only 2000-01-01 ''
"$script" --base "$base" --strict >out 2>&1 || { cat out; echo 'FAIL date-only stale date'; exit 1; }
grep -q 'WARNING: .*stale_after.*past' out || { cat out; echo 'FAIL stale date warning'; exit 1; }
echo 'ok   date-only stale date warns'
rm doc/date-only.md
make_doc fenced 2999-01-01T00:00:00Z ''
# shellcheck disable=SC2016
printf '```text\n[ignored](missing.md)\n```\n' >> doc/fenced.md
"$script" --base "$base" --strict >out 2>&1 || { cat out; echo 'FAIL fenced link checked'; exit 1; }
echo 'ok   fenced link ignored'
rm doc/fenced.md
printf '# Missing metadata\n' > doc/nested/staged.md
git add doc/nested/staged.md
if "$script" --base "$base" --strict >out 2>&1; then echo 'FAIL staged new doc accepted'; exit 1; fi
grep -q 'missing frontmatter' out || { cat out; exit 1; }
echo 'ok   staged new doc requires frontmatter'
git reset -q HEAD -- doc/nested/staged.md
rm doc/nested/staged.md
printf '%s\n' '---' 'ordinary prose without metadata' > README.md
"$script" --base "$base" --strict >out 2>&1 || { cat out; echo 'FAIL optional root frontmatter'; exit 1; }
echo 'ok   root horizontal rule needs no frontmatter'
git checkout -q -- README.md
printf '[guide](doc/missing.md)\n' > AGENTS.md
if "$script" --base "$base" --strict >out 2>&1; then echo 'FAIL broken link in AGENTS.md accepted'; exit 1; fi
grep -q 'missing.md' out || { cat out; exit 1; }
echo 'ok   AGENTS.md is linted'
rm AGENTS.md
