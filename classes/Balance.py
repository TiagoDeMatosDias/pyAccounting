from decimal import Decimal


class Balance:
    def __init__(self, Date=None, Account=None, Balance=None, Type=None, Price=None, FairValue=None, CostBasis=None, normalizedCurrency=None):
        self.Date = str(Date)
        self.Commodity = str(Account)
        self.Type = str(Type)
        self.normalizedCurrency = str(normalizedCurrency)
        if Balance is None:
            self.Balance = None
        else:
            self.Balance = Decimal(Balance)
        if Price is None:
            self.Price = None
        else:
            self.Price = Decimal(Price)
        if FairValue is None:
            self.FairValue = None
        else:
            self.FairValue = Decimal(FairValue)
        if CostBasis is None:
            self.CostBasis = None
        else:
            self.CostBasis = Decimal(CostBasis)
