from collections import deque
from math import ceil
from threading import Lock
from time import monotonic


class IncidentRateLimiter:
    def __init__(
        self,
        min_interval_seconds=3,
        window_seconds=60,
        max_requests=12,
        clock=None,
    ):
        self.min_interval_seconds = max(0, float(min_interval_seconds))
        self.window_seconds = max(1, float(window_seconds))
        self.max_requests = max(1, int(max_requests))
        self.clock = clock or monotonic
        self.events = deque()
        self.lock = Lock()

    def check(self):
        now = float(self.clock())
        with self.lock:
            cutoff = now - self.window_seconds
            while self.events and self.events[0] <= cutoff:
                self.events.popleft()

            if self.events:
                since_last = now - self.events[-1]
                if since_last < self.min_interval_seconds:
                    return False, ceil(self.min_interval_seconds - since_last)

            if len(self.events) >= self.max_requests:
                retry_after = self.window_seconds - (now - self.events[0])
                return False, ceil(max(retry_after, 1))

            self.events.append(now)
            return True, 0
