import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "jobs.db")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            sender TEXT,
            subject TEXT,
            filename TEXT,
            status TEXT,
            created_at TEXT,
            updated_at TEXT,
            error_message TEXT
        )
    """)
    conn.commit()
    conn.close()

def log_job(job_id, sender, subject, filename):
    now = datetime.utcnow().isoformat()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO jobs (job_id, sender, subject, filename, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (job_id, sender, subject, filename, "processing", now, now))
    conn.commit()
    conn.close()

def update_job_status(job_id, status, error_message=None):
    now = datetime.utcnow().isoformat()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE jobs
        SET status = ?, updated_at = ?, error_message = ?
        WHERE job_id = ?
    """, (status, now, error_message, job_id))
    conn.commit()
    conn.close()

def get_all_jobs():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jobs ORDER BY created_at DESC")
    jobs = cursor.fetchall()
    conn.close()
    return jobs
