import time


class TokenBucket:
    def __init__(self, token_list: list, interval: float = 0.2):
        self.token_list = token_list
        self._interval = interval / len(token_list)
        self._last_pop_time = 0
        self._next_pop_index = 0

    def pop(self):
        while True:
            cur_time = time.time()
            if cur_time - self._last_pop_time > self._interval:
                self._last_pop_time = cur_time
                token = self.token_list[self._next_pop_index]
                self._next_pop_index = (self._next_pop_index + 1) % len(self.token_list)
                return token
