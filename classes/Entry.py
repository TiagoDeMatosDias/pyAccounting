from decimal import Decimal


class Entry:
    Date = ""
    Type = ""
    Id = ""
    Name = ""
    Account = ""
    Quantity = Decimal(0.00)
    Quantity_Type = ""
    Cost = Decimal(0.00)
    Cost_Type = ""


    def __init__(self, Date="", Type="", Id="", Name="", Account="", Quantity=Decimal(0.00), Quantity_Type="",
                 Cost=Decimal(0.00), Cost_Type=""):
        if(Quantity == None):
            Quantity = Decimal(0.00)
        if (Cost == None):
            Cost = Decimal(0.00)

        if (Quantity_Type == None):
            Quantity_Type = ""
        if (Cost_Type == None):
            Cost_Type = ""
        if (Account == None):
            Account = ""
        self.Date = str(Date)
        self.Type = str(Type)
        self.Id = str(Id)
        self.Name = str(Name)
        self.Account = str(Account)
        self.Quantity = Decimal(Quantity)
        self.Quantity_Type = str(Quantity_Type)
        self.Cost = Decimal(Cost)
        self.Cost_Type = str(Cost_Type)

    def write_CSV(self, separator):
        output = str(self.Date) + separator
        output += str(self.Type) + separator
        output += str(self.Id) + separator
        output += str(self.Name) + separator
        output += str(self.Account) + separator
        output += str(self.Quantity) + separator
        output += str(self.Quantity_Type) + separator
        output += str(self.Cost) + separator
        output += str(self.Cost_Type)
        return output

    def headers_CSV(self, separator):
        output = "Date" + separator
        output += "Type" + separator
        output += "Id" + separator
        output += "Name" + separator
        output += "Account" + separator
        output += "Quantity" + separator
        output += "Quantity_Type" + separator
        output += "Cost" + separator
        output += "Cost_Type"
        return output

    def sort_priority(self):
        return self.Date