import threading
import time
from datetime import datetime
from typing import Callable, Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import config


class Scheduler:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.scheduler = BackgroundScheduler(timezone=config.TIMEZONE)
        self._is_running = False
        self._job_id = "hpd_sync_job"
        self._sync_callback: Optional[Callable] = None
        self._initialized = True

    def set_sync_callback(self, callback: Callable):
        self._sync_callback = callback

    def start(self):
        if self._is_running:
            return

        for time_str in config.SCHEDULE_TIMES:
            hour, minute = map(int, time_str.split(":"))
            trigger = CronTrigger(hour=hour, minute=minute, timezone=config.TIMEZONE)
            self.scheduler.add_job(
                self._run_sync,
                trigger=trigger,
                id=f"{self._job_id}_{time_str.replace(':', '')}",
                replace_existing=True,
            )

        self.scheduler.start()
        self._is_running = True

    def stop(self):
        if not self._is_running:
            return

        self.scheduler.shutdown(wait=False)
        self._is_running = False

    def _run_sync(self):
        if self._sync_callback:
            try:
                self._sync_callback()
            except Exception as e:
                print(f"Sync error: {e}")

    def run_sync_now(self):
        self._run_sync()

    @property
    def is_running(self) -> bool:
        return self._is_running

    def get_next_run(self) -> Optional[datetime]:
        jobs = self.scheduler.get_jobs()
        if jobs:
            next_times = [job.next_run_time for job in jobs if job.next_run_time]
            return min(next_times) if next_times else None
        return None

    def get_status(self) -> dict:
        return {
            "is_running": self._is_running,
            "next_run": self.get_next_run(),
            "scheduled_times": config.SCHEDULE_TIMES,
            "jobs": [
                {"id": job.id, "next_run": job.next_run_time}
                for job in self.scheduler.get_jobs()
            ],
        }
