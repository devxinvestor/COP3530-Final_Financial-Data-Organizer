class MyHashMap:
    def __init__(self):
        self.size = 16
        self.buckets = [[] for _ in range(self.size)]
        self.num_items = 0
        self.load_factor_threshold = 0.8

    def _hash(self, key):
        return hash(key) % self.size

    def _re_hash(self):
        old_buckets = self.buckets
        self.size *= 2
        self.buckets = [[] for _ in range(self.size)]
        self.num_items = 0

        for bucket in old_buckets:
            for key, value in bucket:
                self.__setitem__(key, value)

    def __setitem__(self, key, value):
        if self.num_items / self.size > self.load_factor_threshold:
            self._re_hash()

        hash_code = self._hash(key)
        bucket = self.buckets[hash_code]
        for i, (k, v) in enumerate(bucket):
            if k == key:
                bucket[i] = (key, value)
                return
        bucket.append((key, value))
        self.num_items += 1

    def __getitem__(self, key):
        hash_code = self._hash(key)
        bucket = self.buckets[hash_code]
        for k, v in bucket:
            if k == key:
                return v
        raise KeyError(f"Key '{key}' not found.")

    def __delitem__(self, key):
        hash_code = self._hash(key)
        bucket = self.buckets[hash_code]
        for i, (k, v) in enumerate(bucket):
            if k == key:
                del bucket[i]
                self.num_items -= 1
                return
        raise KeyError(f"Key '{key}' not found.")

    def __contains__(self, key):
        hash_code = self._hash(key)
        bucket = self.buckets[hash_code]
        for k, v in bucket:
            if k == key:
                return True
        return False

    def __str__(self):
        items = []
        for bucket in self.buckets:
            items.extend(bucket)
        return "{" + ", ".join(f"{k}: {v}" for k, v in items) + "}"