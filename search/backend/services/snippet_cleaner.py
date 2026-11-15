import re


def remove_wiki_media(text: str) -> str:
    """Remove complete File/Image links, including nested wiki links."""
    pattern = re.compile(r"\[\[(?:File|Image):", re.IGNORECASE)

    while True:
        match = pattern.search(text)

        if not match:
            break

        start = match.start()
        depth = 1
        i = match.end()

        while i < len(text) and depth > 0:
            if text.startswith("[[", i):
                depth += 1
                i += 2
            elif text.startswith("]]", i):
                depth -= 1
                i += 2
            else:
                i += 1

        if depth == 0:
            text = text[:start] + " " + text[i:]
        else:
            # Malformed markup: remove the remaining media block.
            text = text[:start]

    return text


def clean_wikipedia_text(text: str, max_length: int = 240) -> str:
    """Remove common Wikipedia markup for readable search snippets."""
    if not text:
        return ""

    text = str(text)

    # Remove comments.
    text = re.sub(r"<!--.*?-->", " ", text, flags=re.DOTALL)

    # Remove references.
    text = re.sub(
        r"<ref\b[^>]*>.*?</ref\s*>",
        " ",
        text,
        flags=re.IGNORECASE | re.DOTALL
    )
    text = re.sub(
        r"<ref\b[^>]*/\s*>",
        " ",
        text,
        flags=re.IGNORECASE
    )

    # Remove complete image and file links.
    text = remove_wiki_media(text)

    # Remove tables.
    text = re.sub(r"\{\|.*?\|\}", " ", text, flags=re.DOTALL)

    # Remove nested templates.
    previous = None
    while previous != text:
        previous = text
        text = re.sub(r"\{\{[^{}]*\}\}", " ", text)

    # Convert piped wiki links to display text.
    text = re.sub(
        r"\[\[[^|\]]+\|([^\]]+)\]\]",
        r"\1",
        text
    )

    # Convert ordinary wiki links.
    text = re.sub(r"\[\[([^\]]+)\]\]", r"\1", text)

    # Remove external-link syntax, preserving labels.
    text = re.sub(r"\[https?://\S+\s+([^\]]+)\]", r"\1", text)
    text = re.sub(r"\[https?://[^\]]+\]", " ", text)

    # Remove HTML tags.
    text = re.sub(r"<[^>]+>", " ", text)

    # Remove bold and italic formatting.
    text = text.replace("'''", "").replace("''", "")

    # Remove heading markup.
    text = re.sub(r"={2,}", " ", text)

    # Normalize whitespace.
    text = re.sub(r"\s+", " ", text).strip()

    if len(text) <= max_length:
        return text

    snippet = text[:max_length].rsplit(" ", 1)[0]
    return snippet.rstrip(" ,;:-") + "..."
