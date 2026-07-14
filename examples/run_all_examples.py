from .client.get import Get
from .string_ops.customer_experience.email_normalization_old import EmailNormalizationOld
from .string_ops.customer_experience.email_normalization_new import EmailNormalizationNew
from .string_ops.customer_experience.partial_extraction_old import PartialExtractionOld
from .string_ops.customer_experience.partial_extraction_new import PartialExtractionNew
from .string_ops.quickstart.string_expressions import StringExpressions
from .string_ops.quickstart.string_ops import StringOps

example_classes = [
    Get,
    EmailNormalizationOld,
    EmailNormalizationNew,
    PartialExtractionNew,
    PartialExtractionOld,
    StringExpressions,
    StringOps,
]

for cls in example_classes:
    example = cls().run()
