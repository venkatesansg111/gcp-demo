"""Tiny package used by the GitHub Actions demonstrations."""


def greeting(name: str = "GitHub Actions") -> str:
    """Return a greeting for the demo workflow."""
    return f"Hello, {name}!"