import hashlib

def compute_fingerprint(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

# Registry functionality deprecated in favor of S3-based parser loading.
# Keeping compute_fingerprint for potential reuse in content matching.
