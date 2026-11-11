from html.parser import HTMLParser

from pipelines.reports.html import (
    Html,
    badge,
    bar_chart,
    element,
    facts,
    link,
    page,
    section,
    table,
    text,
)


class Tags(HTMLParser):
    """The start tags of a document, in order, failing on any unclosed or stray end tag."""

    VOID = frozenset({"meta", "rect", "br"})

    def __init__(self) -> None:
        super().__init__()
        self.tags: list[str] = []
        self.open: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.tags.append(tag)
        if tag not in self.VOID:
            self.open.append(tag)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.tags.append(tag)

    def handle_endtag(self, tag: str) -> None:
        assert self.open.pop() == tag


def tags(markup: str) -> list[str]:
    parser = Tags()
    parser.feed(markup)
    assert parser.open == []
    return parser.tags


def test_values_are_escaped_and_markup_kept() -> None:
    assert text("<b>&") == "&lt;b&gt;&amp;"
    assert text(Html("<b>")) == "<b>"
    assert text(None) == "-"
    assert text(1.23456) == "1.235"
    assert text(12) == "12"


def test_attributes_are_escaped() -> None:
    assert element("a", "x", href='?a="b"').markup == '<a href="?a=&quot;b&quot;">x</a>'
    assert link("run.html", "<run>").markup == '<a href="run.html">&lt;run&gt;</a>'


def test_a_badge_shows_whether_something_passed() -> None:
    assert badge(True).markup == '<span class="badge pass">passed</span>'
    assert badge(False, failed_label="late").markup == '<span class="badge fail">late</span>'


def test_a_table_right_aligns_numbers_and_escapes_text() -> None:
    markup = table(["Name", "Seconds"], [["<x>", 1.5], ["y", None]], caption="note").markup

    assert tags(markup) == [
        "table",
        "caption",
        "thead",
        "tr",
        "th",
        "th",
        "tbody",
        "tr",
        "td",
        "td",
        "tr",
        "td",
        "td",
    ]
    assert '<td class="num">1.500</td>' in markup
    assert "<td>&lt;x&gt;</td>" in markup
    assert "<td>-</td>" in markup
    assert "<caption>note</caption>" in markup


def test_booleans_are_not_numbers_in_a_table() -> None:
    assert "<td>True</td>" in table(["Flag"], [[True]]).markup


def test_facts_pair_each_label_with_its_value() -> None:
    markup = facts([("Status", badge(True)), ("Probes", 3)]).markup

    assert tags(markup) == ["dl", "dt", "dd", "span", "dt", "dd"]
    assert "<dd>3</dd>" in markup


def test_a_section_has_a_heading_and_its_parts() -> None:
    assert section("A & B", Html("<p>x</p>")).markup == "<h2>A &amp; B</h2><p>x</p>"


def test_a_bar_chart_scales_every_bar_to_the_largest_value() -> None:
    markup = bar_chart([("a <1>", 10.0), ("b", 5.0)], unit="s", caption="Medians").markup

    assert tags(markup) == [
        "figure",
        "svg",
        "text",
        "rect",
        "text",
        "text",
        "rect",
        "text",
        "figcaption",
    ]
    assert 'width="370.0"' in markup
    assert 'width="185.0"' in markup
    assert "a &lt;1&gt;" in markup
    assert "10.0 s" in markup


def test_an_empty_bar_chart_draws_nothing() -> None:
    assert bar_chart([], unit="s").markup == ""
    assert 'width="1.0"' in bar_chart([("zero", 0.0)], unit="s").markup


def test_a_page_is_a_complete_document_without_scripts_or_external_assets() -> None:
    document = page("Runs <all>", Html("<p>body</p>"), subtitle="2 runs")

    assert document.startswith("<!doctype html>\n")
    assert tags(document) == [
        "html",
        "head",
        "meta",
        "meta",
        "title",
        "style",
        "body",
        "h1",
        "p",
        "p",
    ]
    assert "<title>Runs &lt;all&gt;</title>" in document
    assert "<script" not in document
    assert "http" not in document.replace('xmlns="http://www.w3.org/2000/svg"', "")
