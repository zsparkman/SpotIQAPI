# main_parser.py

import os
import shutil

UNHANDLED_DIR = "unhandled_logs"

def save_to_unhandled(file_path):
    """
    Copies the file into the unhandled_logs/ directory for later training.
    """
    os.makedirs(UNHANDLED_DIR, exist_ok=True)
    dest = os.path.join(UNHANDLED_DIR, os.path.basename(file_path))
    shutil.copy(file_path, dest)
    print(f"[↪] Saved unhandled log to {dest}")
