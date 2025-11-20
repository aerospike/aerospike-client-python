import re

INPUT = "my.supp"
OUTPUT = "my_trimmed.supp"
MAX_LINES = 500

with open(INPUT) as f:
    data = f.read()

# Extract blocks including the braces
matches = list(re.finditer(r"\{([^}]*)\}", data, flags=re.DOTALL))

output = data
offset = 0  # track replacement length changes

for m in matches:
    start, end = m.span()
    block = m.group(0)

    lines = block.splitlines()
    if len(lines) <= MAX_LINES:
        continue  # nothing to do

    # Keep first 240 lines and last 240 lines + brace line
    keep_head = 240
    keep_tail = 240

    new_lines = (
        lines[:keep_head]
        + ["   # ... trimmed ..."]
        + lines[-keep_tail:]
    )

    new_block = "\n".join(new_lines)

    # Replace block inside the full text
    output = output[:start + offset] + new_block + output[end + offset:]
    offset += len(new_block) - (end - start)

with open(OUTPUT, "w") as f:
    f.write(output)

print("Done. Wrote:", OUTPUT)
