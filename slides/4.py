import selectors

class Loop:
    _selector: selectors.BaseSelector # = selectors.DefaultSelector()

    # Show registration only.
    # Real implementation has to either register or modify fd readiness mask
    # and later dispatch depending on event type
    def add_reader(self, fd, callback, *args):
        self._selector.register(fd, selectors.EVENT_READ,
                                data={selectors.EVENT_READ: (callback, args)})

    def add_writer(self, fd, callback, *args):
        self._selector.register(fd, selectors.EVENT_WRITE,
                                data={selectors.EVENT_WRITE: (callback, args)})

    def remove_reader(self, fd): ...
    def remove_writer(self, fd): ...
