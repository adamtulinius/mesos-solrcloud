class Agent:
    def __init__(self, id=None, hostname=None):
        self.id = id
        self.hostname = hostname

    def __str__(self):
        return "%s (%s)" % (self.hostname, self.id)

    def render(self):
        return {'value': self.id}