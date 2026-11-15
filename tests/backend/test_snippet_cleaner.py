import pytest

from search.backend.services.snippet_cleaner import clean_wikipedia_text


@pytest.mark.parametrize(
    "raw, expected",
    [
        (
            "'''Computer science''' is the study of computation.",
            "Computer science is the study of computation."
        ),
        (
            "Related to [[Mathematics|mathematics]] and logic.",
            "Related to mathematics and logic."
        ),
        (
            "See [[Alan Turing]] for more information.",
            "See Alan Turing for more information."
        ),
        (
            "Alan Turing was a mathematician.<ref>Source citation</ref>",
            "Alan Turing was a mathematician."
        ),
        (
            "Before {{Infobox person|name=Alan Turing}} after.",
            "Before after."
        ),
        (
            "Text before [[File:example.jpg|thumb|An image]] text after.",
            "Text before text after."
        ),
        (
            "Read [https://example.com the official website].",
            "Read the official website."
        ),
    ]
)
def test_clean_wikipedia_text(raw, expected):
    assert clean_wikipedia_text(raw) == expected


def test_empty_text():
    assert clean_wikipedia_text("") == ""


def test_none_text():
    assert clean_wikipedia_text(None) == ""


def test_snippet_length_limit():
    text = " ".join(["computer"] * 100)

    result = clean_wikipedia_text(text, max_length=50)

    assert len(result) <= 53
    assert result.endswith("...")


def test_plain_text_is_preserved():
    text = "Computer science studies computation and information."

    assert clean_wikipedia_text(text) == text


def test_nested_links_inside_file_markup_are_removed():
    raw = (
        "[[File:Alan Turing Memorial Closer.jpg|right|thumb|"
        "A [[statue]] of Alan Turing]] "
        "'''Alan Mathison Turing''' was an English "
        "[[mathematician]] and [[computer scientist]]."
    )

    expected = (
        "Alan Mathison Turing was an English "
        "mathematician and computer scientist."
    )

    assert clean_wikipedia_text(raw, max_length=500) == expected
