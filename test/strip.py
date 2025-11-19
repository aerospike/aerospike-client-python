import re

input_file = "val.log"
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

            block_str = "\n".join(current_block)
            unique_blocks.add(block_str)
        elif inside_block:
            current_block.append(line)

# Write unique blocks
with open(output_file, "w") as f:
    for block in unique_blocks:
        f.write(block + "\n\n")

print(f"Original number of suppressions: {total_blocks}")
print(f"Number of unique suppressions: {len(unique_blocks)}")
