import spacy

nlp = spacy.load("en_core_web_sm")

def preprocess_text(text: str) -> str:
    """
    Cleans text: Remove stop words, URSs, normalize.
    """
    doc = nlp(text)
    cleaned = ' '.join(token.text.lower() for token in doc if not token.is_stop and not token.like_url and not token.is_punct)
    return cleaned