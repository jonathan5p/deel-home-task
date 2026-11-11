from collections.abc import Sequence
from dataclasses import dataclass
from html import escape

MISSING = "-"
CHART_WIDTH = 640
CHART_LABEL_WIDTH = 200
CHART_VALUE_WIDTH = 70
CHART_BAR_HEIGHT = 18
CHART_GAP = 6

STYLE = """
:root { color-scheme: light dark; --fg: #1f2328; --muted: #59636e; --line: #d1d9e0;
  --head: #f6f8fa; --pass: #1a7f37; --fail: #cf222e; --bar: #0969da; }
@media (prefers-color-scheme: dark) {
  :root { --fg: #e6edf3; --muted: #9198a1; --line: #3d444d; --head: #151b23;
    --pass: #3fb950; --fail: #f85149; --bar: #4493f8; }
}
body { font: 14px/1.5 system-ui, sans-serif; color: var(--fg); margin: 2rem auto;
  max-width: 1100px; padding: 0 1rem; }
h1 { font-size: 1.6rem; margin-bottom: 0.2rem; }
h2 { font-size: 1.15rem; margin-top: 2rem; border-bottom: 1px solid var(--line); }
.subtitle, figcaption, caption { color: var(--muted); }
caption { caption-side: bottom; text-align: left; padding-top: 0.3rem; }
table { border-collapse: collapse; margin: 0.5rem 0; }
th, td { border: 1px solid var(--line); padding: 0.25rem 0.6rem; text-align: left; }
th { background: var(--head); }
td.num { text-align: right; font-variant-numeric: tabular-nums; }
dl { display: grid; grid-template-columns: max-content auto; gap: 0.2rem 1rem; }
dt { color: var(--muted); }
dd { margin: 0; }
.badge { display: inline-block; padding: 0 0.6rem; border-radius: 1rem; color: #fff;
  font-weight: 600; }
.badge.pass { background: var(--pass); }
.badge.fail { background: var(--fail); }
svg text { fill: var(--fg); font-size: 12px; }
svg rect { fill: var(--bar); }
"""


@dataclass(frozen=True, slots=True)
class Html:
    """Markup that is safe to insert as is; every other value is escaped as text."""

    markup: str


def text(value: object) -> str:
    """A cell or field value as escaped text: `-` for None, three decimals for a float."""
    match value:
        case Html(markup):
            return markup
        case None:
            return MISSING
        case float():
            return f"{value:.3f}"
        case _:
            return escape(str(value))


def join(parts: Sequence[Html]) -> Html:
    return Html("".join(part.markup for part in parts))


def element(tag: str, content: object, **attributes: str) -> Html:
    rendered = "".join(f' {name}="{escape(value)}"' for name, value in attributes.items())
    return Html(f"<{tag}{rendered}>{text(content)}</{tag}>")


def link(href: str, label: object) -> Html:
    return element("a", label, href=href)


def badge(passed: bool, *, passed_label: str = "passed", failed_label: str = "failed") -> Html:
    return element(
        "span",
        passed_label if passed else failed_label,
        **{"class": "badge pass" if passed else "badge fail"},
    )


def _cell(value: object) -> Html:
    numeric = isinstance(value, int | float) and not isinstance(value, bool)
    return element("td", value, **{"class": "num"}) if numeric else element("td", value)


def table(headers: Sequence[str], rows: Sequence[Sequence[object]], *, caption: str = "") -> Html:
    """A table; numbers are right-aligned, None shows as `-`."""
    head = join([element("th", header) for header in headers])
    body = join([element("tr", join([_cell(value) for value in row])) for row in rows])
    note = element("caption", caption).markup if caption else ""
    return Html(
        f"<table>{note}<thead><tr>{head.markup}</tr></thead><tbody>{body.markup}</tbody></table>"
    )


def facts(items: Sequence[tuple[str, object]]) -> Html:
    """A list of labelled values, such as a run's settings."""
    pairs = join([join([element("dt", name), element("dd", value)]) for name, value in items])
    return Html(f"<dl>{pairs.markup}</dl>")


def section(title: str, *parts: Html) -> Html:
    return Html(element("h2", title).markup + join(parts).markup)


def bar_chart(bars: Sequence[tuple[str, float]], *, unit: str, caption: str = "") -> Html:
    """A horizontal bar chart as inline SVG, one labelled bar per value, scaled to the largest."""
    if not bars:
        return Html("")
    largest = max(value for _, value in bars) or 1.0
    span = CHART_WIDTH - CHART_LABEL_WIDTH - CHART_VALUE_WIDTH
    step = CHART_BAR_HEIGHT + CHART_GAP
    shapes = []
    for index, (label, value) in enumerate(bars):
        y = index * step
        width = max(1.0, value / largest * span)
        middle = y + CHART_BAR_HEIGHT * 0.75
        shapes.append(
            f'<text x="0" y="{middle:.1f}">{escape(label)}</text>'
            f'<rect x="{CHART_LABEL_WIDTH}" y="{y}" width="{width:.1f}"'
            f' height="{CHART_BAR_HEIGHT}"/>'
            f'<text x="{CHART_LABEL_WIDTH + width + 4:.1f}" y="{middle:.1f}">'
            f"{value:.1f} {escape(unit)}</text>"
        )
    height = len(bars) * step
    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{CHART_WIDTH}" height="{height}"'
        f' viewBox="0 0 {CHART_WIDTH} {height}" role="img">{"".join(shapes)}</svg>'
    )
    note = element("figcaption", caption).markup if caption else ""
    return Html(f"<figure>{svg}{note}</figure>")


def page(title: str, *parts: Html, subtitle: str = "") -> str:
    """A complete, self-contained HTML document: inline CSS, no scripts or external assets, so
    it opens straight from object storage or as a local copy."""
    header = element("h1", title).markup
    if subtitle:
        header += element("p", subtitle, **{"class": "subtitle"}).markup
    return (
        "<!doctype html>\n"
        '<html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"<title>{escape(title)}</title><style>{STYLE}</style></head>"
        f"<body>{header}{join(parts).markup}</body></html>\n"
    )
