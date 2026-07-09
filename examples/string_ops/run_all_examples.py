from .customer_experience.email_normalization_old import EmailNormalizationOld
from .customer_experience.email_normalization_new import EmailNormalizationNew
from .customer_experience.partial_extraction_old import PartialExtractionOld
from .customer_experience.partial_extraction_new import PartialExtractionNew
from .quickstart.string_expressions import StringExpressions
from .quickstart.string_ops import StringOps

example_classes = [
    EmailNormalizationOld,
    EmailNormalizationNew,
    PartialExtractionNew,
    PartialExtractionOld,
    StringExpressions,
    StringOps
]

for cls in example_classes:
    example = cls().run()
