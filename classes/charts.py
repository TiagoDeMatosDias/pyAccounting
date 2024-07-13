import matplotlib as plt
import seaborn as sns
def generate_pieChart(df, Columns_Name, Values_Name, SaveLocation):
     fig, ax = plt.subplots(figsize=(10, 5))
     plt.pie(df[Values_Name], labels=df[Columns_Name], autopct='%1.1f%%', pctdistance=1.25, labeldistance=1.6)
     fig.savefig(SaveLocation)


def generate_stackedBarChart(data, Index_Name, Columns_Name, Values_Name, SaveLocation, Title, colormap):
     # Pivot the dataframe
     data_pivot = data.pivot(index=Index_Name, columns=Columns_Name, values=Values_Name).fillna(0)

     # Create a larger figure
     fig, ax = plt.subplots(figsize=(12, 8))

     # Create a stacked bar chart
     data_pivot.plot(kind='bar', stacked=True, colormap=colormap, ax=ax)

     # Customize the plot
     ax.set_title(Title )
     ax.set_xlabel(Index_Name)
     ax.set_ylabel(Values_Name)

     # Remove the legend
     ax.legend().set_visible(False)

     # Save the plot to a file
     fig.savefig(SaveLocation)