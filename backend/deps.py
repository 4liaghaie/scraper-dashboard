# deps.py
from jobs.manager import JobManager
from scheduler import build_scheduler  

# a single, shared JobManager instance for the whole app
job_manager = JobManager()
scheduler = build_scheduler()           # <-- add this
