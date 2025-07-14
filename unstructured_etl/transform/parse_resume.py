import re

def parse_resume_text(text):
    """
    Parses extracted resume text to pull name, email, phone, experience, summary.
    Uses regex and simple logic.
    """
    # Email
    email_match = re.search(r"[\w\.-]+@[\w\.-]+", text)
    email = email_match.group(0) if email_match else None

    # Phone (10-12 digit patterns)
    phone_match = re.search(r"\b\d{10,12}\b", text)
    phone = phone_match.group(0) if phone_match else None

    # Experience (e.g., "3 years", "5+ years", etc.)
    exp_match = re.search(r"(\d+)\+?\s*years?", text, re.IGNORECASE)
    experience = exp_match.group(0) if exp_match else None

    # Name (assume first line is name if it has 2 capitalized words)
    lines = text.strip().splitlines()
    name = None
    for line in lines:
        if len(line.split()) == 2 and all(w[0].isupper() for w in line.split()):
            name = line.strip()
            break

    # Summary (first 1–2 lines of meaningful text)
    summary = ""
    for line in lines:
        if len(line.strip()) > 40:
            summary = line.strip()
            break

    return {
        "name": name or "Not found",
        "email": email or "Not found",
        "phone": phone or "Not found",
        "experience": experience or "Not found",
        "summary": summary or "No summary found"
    }
