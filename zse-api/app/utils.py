# Utility functions
def format_currency(value):
    if value is None:
        return "N/A"
    return f"{value:,.2f}"
