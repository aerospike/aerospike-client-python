import re

input_file = "sample.txt"
output_file = "unique_suppressions.txt"

unique_blocks = set()
current_block = []
inside_block = False
total_blocks = 0

ts_re = re.compile(r"^\d{4}-\d{2}-\d{2}T\S+\s+(.*)$")

with open(input_file) as f:
    for raw_line in f:
        line = raw_line.rstrip()

        # Strip timestamp prefix if present
        m = ts_re.match(line)
        if m:
            line = m.group(1)

        if line.startswith("{"):
            inside_block = True
            current_block = [line]
        elif line.startswith("}"):
            current_block.append(line)
            inside_block = False
            total_blocks += 1

            block_str = "\n".join(l.rstrip() for l in current_block)
            unique_blocks.add(block_str)
        elif inside_block:
            current_block.append(line)

# Write unique blocks with tabs for inner lines only
with open(output_file, "w") as f:
    for block in sorted(unique_blocks):
        lines = block.split("\n")
        # Keep first line `{` as is, indent all lines in between, last line `}` as is
        if len(lines) > 2:
            indented_block = "\n".join(
                [lines[0]] + ["\t" + line for line in lines[1:-1]] + [lines[-1]]
            )
        else:
            # Blocks with only {} or {} plus one line
            indented_block = "\n".join(lines)
        f.write(indented_block + "\n\n")

print(f"Original number of suppressions: {total_blocks}")
print(f"Number of unique suppressions: {len(unique_blocks)}")
