# Writing style

Use these rules for new and edited documentation for B2 Command Line Tool.
They do not apply to `CHANGELOG.md`, released `.changes/v*.md` files, or generated
`doc/source/subcommands/` pages. The source for those generated pages is the command
help text in `b2/_internal/console_tool.py`.

## Rules

1. **Do:** Put one idea in each sentence. Aim for 20 words or fewer in steps and 25 or fewer in descriptions.
   **Instead of:** Start the upload and check the result. **Write:** Start the upload. Check the result.
2. **Do:** Use active voice and name the actor.
   **Instead of:** The file is uploaded by the CLI. **Write:** The CLI uploads the file.
3. **Do:** Write steps as commands, with one action per step.
   **Instead of:** You set the option and run the command. **Write:** Set the option. Run the command.
4. **Do:** Use the terms in the table consistently.
   **Instead of:** Upload the item. **Write:** Upload the file.
5. **Do:** Use simple verbs and avoid idioms.
   **Instead of:** Kick off the upload. **Write:** Start the upload.
6. **Do:** Keep articles such as “a” and “the”.
   **Instead of:** Set option for bucket. **Write:** Set the option for the bucket.
7. **Do:** Use plain words and avoid empty intensifiers.
   **Instead of:** Simply upload the file. **Write:** Upload the file.
8. **Do:** Repeat a noun when a pronoun could refer to more than one thing.
   **Instead of:** The command reads the file before it starts. **Write:** The command reads the file before the upload starts.
9. **Do:** Preserve the strength of “must”, “should”, and “can”: requirement, recommendation, and ability respectively.
   **Instead of:** You should provide a required value. **Write:** You must provide the required value.
10. **Do:** Put the condition before the action.
    **Instead of:** Check the output if the command fails. **Write:** If the command fails, check the output.
11. **Do:** Give exact numbers and units only when they are verified.
    **Instead of:** Wait a few seconds. **Write:** Wait 3 seconds. Use 3 seconds only when it is known to be correct.
12. **Do:** Keep paragraphs to about six sentences or fewer. Use a list for a sequence.
    **Instead of:** First set the option. Next run the command. **Write:** 1. Set the option. 2. Run the command.
13. **Do:** Preserve public interfaces and literal output in wording changes.
    **Instead of:** Rename `b2v5` for style. **Write:** Keep `b2v5` unchanged.
14. **Do:** Change wording without changing meaning.
    **Instead of:** Replace a requirement with a recommendation. **Write:** Keep the requirement.

## Terms

| Concept | Terms |
| --- | --- |
| Credentials | Use “application key” for the credential used to authorize; use “authorization token” for a token, such as a download authorization token. |
| Storage container | Use “bucket”. |
| Stored data | Use “file” for CLI file commands and file versions; use “object” when describing an object or quoting an API that uses that term. Distinguish a local file from a B2 file when needed. |
| Address | Use “B2 URI” for the `b2://` and `b2id://` forms. Use the appropriate form for the command being described. |
| CLI interface version | Use “ApiVer” for the interface version; `b2v5` selects the version-bound ApiVer 5 interface. |

## Meaning-preservation checklist

A wording change must never change command or option names, environment variable names,
`b2://` or `b2id://` URI forms, JSON keys, exit codes, error strings, capability names,
or code blocks. Preserve numbers, units, limits, security and compatibility statements.
Keep the strength of “must”, “should”, and “can”. Verify claims against the code or B2
contract; flag uncertainty rather than guessing or concealing a contradiction.

Changelog entries follow the existing repository rule: write one complete sentence that
starts with a capital letter and ends with a full stop.

## How to check

Review sentence length, passive voice, and ambiguous pronouns. Check each example and
technical term against the repository. Confirm that the meaning and protected items stay
unchanged.
