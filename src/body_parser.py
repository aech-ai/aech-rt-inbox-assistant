"""
Email body parsing - HTML to semantic markdown conversion.

Converts raw HTML email bodies to markdown. That's it.
LLM handles the smart stuff (signature extraction, quote removal).
"""

from dataclasses import dataclass

from markdownify import markdownify as md
import re


@dataclass
class ParsedBody:
    """Parsed email body components."""
    main_content: str      # Full markdown content
    signature_block: str   # Empty - LLM extracts this
    raw_html: str          # Original HTML for reference


def html_to_markdown(html: str) -> str:
    """
    Convert HTML to semantic markdown.

    Preserves: headers, lists, bold/italic, links, paragraph breaks
    Strips: tables, images, inline styles, HTML comments
    """
    if not html:
        return ""

    # Strip HTML comments (often contain CSS, conditionals, etc.)
    html = re.sub(r'<!--[\s\S]*?-->', '', html)

    # Convert with markdownify
    markdown = md(
        html,
        heading_style="atx",
        strip=['table', 'img', 'style', 'script'],
        bullets="-",
    )

    # Clean up excessive whitespace
    markdown = re.sub(r'\n{3,}', '\n\n', markdown)
    markdown = re.sub(r' {2,}', ' ', markdown)
    markdown = markdown.strip()

    return markdown


def parse_email_body(html: str) -> ParsedBody:
    """
    Parse HTML email body to markdown.

    Just converts HTML to markdown. LLM handles:
    - Quote/reply removal
    - Signature extraction
    - Thread summary
    """
    if not html:
        return ParsedBody(main_content="", signature_block="", raw_html="")

    markdown = html_to_markdown(html)

    return ParsedBody(
        main_content=markdown,
        signature_block="",  # LLM populates this via extract-content
        raw_html=html,
    )
