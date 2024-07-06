import matplotlib as plt
import seaborn as sns
def generate_pieChart(df, column_labels, column_values, SaveLocation):
     fig, ax = plt.subplots(figsize=(10, 5))
     plt.pie(df[column_values], labels=df[column_labels], autopct='%1.1f%%', pctdistance=1.25, labeldistance=1.6)
     fig.savefig(SaveLocation)