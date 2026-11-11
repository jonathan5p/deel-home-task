import re

STRING = re.compile(r'"(?:[^"\\]|\\.)*"' + r"|'(?:[^'\\]|\\.)*'")
MATCHERS = re.compile(r"\{[^}]*\}")
RANGE = re.compile(r"\[[^\]]*\]")
GROUPING = re.compile(
    r"\b(?:by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)", re.IGNORECASE
)
VARIABLE = re.compile(r"\$\{?\w+\}?")
IDENTIFIER = re.compile(r"(?<![\w.$])([a-zA-Z_:][a-zA-Z0-9_:]*+)(?!\s*\()")
KEYWORDS = frozenset(
    {"and", "or", "unless", "bool", "offset", "by", "without", "on", "ignoring", "inf", "nan"}
)
RANGE_SELECTOR = re.compile(r"\[([^\]]*)\]")


def metric_names(expr: str) -> set[str]:
    """The metric names a PromQL expression selects: every identifier that is not a function,
    a keyword, a label in a matcher or grouping clause, a duration or a Grafana variable."""
    stripped = STRING.sub(" ", expr)
    for pattern in (MATCHERS, RANGE, GROUPING, VARIABLE):
        stripped = pattern.sub(" ", stripped)
    return {name for name in IDENTIFIER.findall(stripped) if name.lower() not in KEYWORDS}


def ranges(expr: str) -> list[str]:
    """The windows of the expression's range selectors and subqueries, such as `5m` or `1h:`."""
    return RANGE_SELECTOR.findall(STRING.sub(" ", expr))
