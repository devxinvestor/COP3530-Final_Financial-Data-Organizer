class MyHashMap:
    def __init__(self, initial_data=None):
        self.size = 10
        self.buckets = [[] for _ in range(self.size)]
        self.num_items = 0
        self.load_factor_threshold = 0.8
        self.order = []

        if initial_data is not None:
            if isinstance(initial_data, dict):
                for key, value in initial_data.items():
                    self.__setitem__(key, value)
            else:
                raise TypeError("initial_data must be a dictionary")

    def _hash(self, key):
        return hash(key) % self.size

    def _re_hash(self):
        old_buckets = self.buckets
        old_order = self.order[:]
        self.size *= 2
        self.buckets = [[] for _ in range(self.size)]
        self.num_items = 0
        self.order = []

        for key in old_order:
            for bucket in old_buckets:
                for k, v in bucket:
                    if k == key:
                        self.__setitem__(k, v)

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
        self.order.append(key)
        self.num_items += 1

    def __getitem__(self, key):
        hash_code = self._hash(key)
        bucket = self.buckets[hash_code]
        for k, v in bucket:
            if k == key:
                return v
        raise KeyError(f"Key {key} not found")

    def __contains__(self, key):
        if isinstance(key, list):
            return False
        hash_code = self._hash(key)
        bucket = self.buckets[hash_code]
        for k, _ in bucket:
            if k == key:
                return True
        return False

    def __repr__(self):
        items = []
        for bucket in self.buckets:
            items.extend(bucket)
        return f"MyHashMap({items})"
    
    def remove(self, key):
        if isinstance(key, list):
            raise TypeError("Key cannot be a list")
        hash_code = self._hash(key)
        bucket = self.buckets[hash_code]
        for i, (k, v) in enumerate(bucket):
            if k == key:
                del bucket[i]
                self.num_items -= 1
                self.order.remove(key)
                return
        raise KeyError(f"Key {key} not found")

    def to_dataframe_data(self):
        columns = []
        values = []
        for key in self.order:
            hash_code = self._hash(key)
            bucket = self.buckets[hash_code]
            for k, v in bucket:
                if k == key:
                    columns.append(key)
                    values.append(v)
                    break
        
        return columns, values