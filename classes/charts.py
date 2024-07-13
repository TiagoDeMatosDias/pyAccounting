import matplotlib.pyplot as plt
import seaborn as sns
def generate_pieChart(df, Columns_Name, Values_Name, SaveLocation):
     fig, ax = plt.subplots(figsize=(10, 5))
     plt.pie(df[Values_Name], labels=df[Columns_Name], autopct='%1.1f%%', pctdistance=1.25, labeldistance=1.6)
     fig.savefig(SaveLocation)


def generate_stackedBarChart(data, Index_Name, Columns_Name, Values_Name, SaveLocation, Title, colormap,max_legend_entries):

     # Pivot the dataframe
     data_pivot = data.pivot_table(index=Index_Name, columns=Columns_Name, values=Values_Name, aggfunc="sum").fillna(0)

     # Create a larger figure
     fig, ax = plt.subplots(figsize=(20, 15))

     # Create a stacked bar chart
     data_pivot.plot(kind='bar', stacked=True, colormap=colormap, ax=ax, label='Inline label')

     # Customize the plot
     ax.set_title(Title )
     ax.set_xlabel(Index_Name)
     ax.set_ylabel(Values_Name)

     # Customize the legend to show up to 10 entries
     # Sort columns by the sum of values and keep only the top max_legend_entries
     top_columns = data_pivot.sum().nlargest(max_legend_entries).index

     # Add the legend with a limited number of entries
     # Customize the legend to show up to 10 entries
     handles, labels = ax.get_legend_handles_labels()
     if len(handles) > max_legend_entries:
          ax.legend(top_columns, top_columns, title=Columns_Name, loc='best')
     else:
          ax.legend(title=Columns_Name, loc='best')

     # Save the plot to a file
     fig.savefig(SaveLocation)