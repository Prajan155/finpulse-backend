import threading
import time
from datetime import datetime

from app.services.news_service import refresh_news_knowledge

NEWS_REFRESH_INTERVAL = 60 * 15
STARTUP_DELAY = 10


def _run_periodic_job(interval, job_fn, job_name):
    while True:
        try:
            print(f"🔥 [{job_name}] Running at {datetime.utcnow().isoformat()}")
            result = job_fn()
            print(f"✅ [{job_name}] Result: {result}")
        except Exception as exc:
            print(f"❌ [{job_name}] Failed: {exc}")

        time.sleep(interval)


def _initial_bootstrap():
    try:
        print(f"🔥 [NEWS BOOTSTRAP] Running at {datetime.utcnow().isoformat()}")
        news_result = refresh_news_knowledge()
        print(f"✅ [NEWS BOOTSTRAP] Result: {news_result}")
    except Exception as exc:
        print(f"❌ [NEWS BOOTSTRAP] Failed: {exc}")


def start_scheduler():
    def delayed_start():
        time.sleep(STARTUP_DELAY)
        print("🔥 FirePulse Scheduler Started")

        _initial_bootstrap()

        threading.Thread(
            target=_run_periodic_job,
            args=(NEWS_REFRESH_INTERVAL, refresh_news_knowledge, "NEWS REFRESH"),
            daemon=True,
        ).start()

    threading.Thread(target=delayed_start, daemon=True).start()