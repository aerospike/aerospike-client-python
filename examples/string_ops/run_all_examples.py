from .customer_experience.email_normalization import EmailNormalization
from .customer_experience.partial_extraction import PartialExtraction
from .quickstart.string_expressions import StringExpressions
from .quickstart.string_ops import StringOps

example_classes = [
    EmailNormalization,
    PartialExtraction,
    StringExpressions,
    StringOps
]

for cls in example_classes:
    example = cls()
    example.run()
