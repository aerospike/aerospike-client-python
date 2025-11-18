input_file = "val.log"
output_file = "unique_suppressions.txt"

unique_blocks = set()
current_block = []
inside_block = False
total_blocks = 0  # count all blocks

with open(input_file) as f:
    for line in f:
        line = line.rstrip()
        if line.startswith("{"):
            inside_block = True
            current_block = [line]
        elif line.startswith("}"):
            current_block.append(line)
            inside_block = False
            block_str = "\n".join(current_block)
            unique_blocks.add(block_str)
            total_blocks += 1
        elif inside_block:
            current_block.append(line)

# Write all unique blocks to output
with open(output_file, "w") as f:
    for block in unique_blocks:
        f.write(block + "\n\n")

# Print statistics
print(f"Original number of suppressions: {total_blocks}")
print(f"Number of unique suppressions: {len(unique_blocks)}")
