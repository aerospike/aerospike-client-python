import re
import sys

if len(sys.argv) != 3:
    print("Usage: python3 filter_suppressions.py <input.supp> <output.supp>")
    sys.exit(1)

input_path = sys.argv[1]
output_path = sys.argv[2]

# Positive match patterns
positive_re = re.compile(r"fun:([A-Za-z0-9_]*?(?:as_.*|Aerospike.*))", re.IGNORECASE)

# Functions you want to skip *unless other good matches exist*
skip_functions = [
    "dotted_as_name_rule",
    "import_from_as_name_rule",
    "import_from_as_names_rule",
    "dotted_as_names_rule",
    "method_output_as_list"
]

block = []
inside = False
matches = []
suppressions_written = 0

with open(input_path) as f:
    for line in f:
        stripped = line.rstrip()

        if stripped.startswith("{"):
            block = [line]
            inside = True
            matches = []
            continue

        if inside and stripped.startswith("}"):
            block.append(line)

            # Determine if block should be kept:
            # 1) Must have at least one positive match
            # 2) If *all* matches are in the skip list → skip block
            # 3) If any match is NOT in skip list → keep block
            if matches:
                print(matches)
                unskiped_matches = [m for m in matches if m not in skip_functions]
                print(unskiped_matches)  
                if unskiped_matches:
                    with open(output_path, "a") as out:
                        out.write("".join(block) + "\n")
                    suppressions_written += 1

            inside = False
            continue

        if inside:
            block.append(line)
            m = positive_re.search(stripped)
            if m:
                func = m.group(1)
                # Strip possible "fun:" prefix
                func = func.replace("fun:", "")
                matches.append(func)

print(f"Total suppressions written: {suppressions_written}")
