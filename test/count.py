import re

with open("my.supp") as f:
    data = f.read()

blocks = re.findall(r"\{([^}]*)\}", data, flags=re.DOTALL)

count = sum(
    1 for b in blocks
    if sum(1 for l in b.splitlines() if l.strip()) > 500
)

print(count)
