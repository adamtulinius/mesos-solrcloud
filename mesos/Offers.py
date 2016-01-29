class Offer:
    def __init__(self, id=None, agent=None, resources=None):
        self.id = id
        self.agent = agent
        self.resources = resources

    def __str__(self):
        return "%s (%s)" % (self.id, self.agent)