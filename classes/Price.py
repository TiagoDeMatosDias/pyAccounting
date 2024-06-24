from decimal import Decimal


class Price:
    def __init__(self, Date=None, Commodity=None, Price=None, Type=None):
        self.Date = str(Date)
        self.Commodity = str(Commodity)
        self.Type = str(Type)
        if Price is None:
            self.Price = None
        else:
            self.Price = Decimal(Price)