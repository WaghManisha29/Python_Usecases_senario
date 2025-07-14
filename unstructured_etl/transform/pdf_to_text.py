from pdfminer.high_level import extract_text

def extract_text_from_pdf(pdf_path):
    """
    Extract raw text content from a PDF file.
    
    Args:
        pdf_path (str): Path to the PDF file.
        
    Returns:
        str: Extracted text content.
    """
    try:
        text = extract_text(pdf_path)
        return text.strip()
    except Exception as e:
        print(f"❌ Error reading {pdf_path}: {e}")
        return ""

if __name__ == "__main__":
    sample_pdf = "../downloads/sample_resume.pdf"
    content = extract_text_from_pdf(sample_pdf)
    print(content)
