class MyDictionary:
    def __init__(self):
        self.items = []
    
    def __setitem__(self, key, value):
        for i, (k, v) in enumerate(self.items):
            if k == key:
                self.items[i] = (key, value)
                return
        self.items.append((key, value))
    
    def __getitem__(self, key):
        for k, v in self.items:
            if k == key:
                return v
        raise KeyError(f"Key '{key}' not found")

    def __delitem__(self, key):
        for i, (k, v) in enumerate(self.items):
            if k == key:
                del self.items[i]
                return
        raise KeyError(f"Key '{key}' not found.")
    
    def __contains__(self, key):
        for k, v in self.items:
            if k == key:
                return True
        return False

    def __str__(self):
        return "{" + ", ".join(f"{k}: {v}" for k, v in self.items) + "}"
