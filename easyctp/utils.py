def dict_iter(self):
    for k, _ in self._fields_:
        v = getattr(self, k)
        if not isinstance(v, bytes):
            yield k, v
        else:
            yield k, v.decode()