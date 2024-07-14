import matplotlib.pyplot as plt
import seaborn as sns
def generate_pieChart(data, Columns_Name, Values_Name, SaveLocation, Title, colormap):
     # Sort the data by size so we have a progression
     data = data.sort_values(by=[Values_Name])

     # Create a pie chart
     fig, ax = plt.subplots(figsize=(20, 15))
     plt.pie(
          data[Values_Name],
          labels=data[Columns_Name],
          autopct='%1.1f%%',
          pctdistance=1.25,
          labeldistance=1.6,
          colors= sns.color_palette(colormap),
          startangle=90
          )
     ax.set_title(Title )

     # We add a hole in the middle
     hole = plt.Circle((0, 0), 0.45, facecolor='white')
     plt.gcf().gca().add_artist(hole)

     # Save the plot to a file
     fig.savefig(SaveLocation)


def generate_stackedBarChart(data, Index_Name, Columns_Name, Values_Name, SaveLocation, Title, colormap,max_legend_entries):

     # Pivot the dataframe
     data_pivot = data.pivot_table(index=Index_Name, columns=Columns_Name, values=Values_Name, aggfunc="sum").fillna(0)

     # Create a larger figure
     fig, ax = plt.subplots(figsize=(20, 15))

     # Create a stacked bar chart
     data_pivot.plot(
          kind='bar',
          stacked=True,
          ax=ax,
          label='Inline label'
     )

     # Customize the plot
     ax.set_title(Title )
     ax.set_xlabel(Index_Name)
     ax.set_ylabel(Values_Name)
     ax = get_cappedHandlesandLabels(ax, data_pivot, max_legend_entries, Columns_Name)

     data_pivot.plot(colormap=colormap)

     # Save the plot to a file
     fig.savefig(SaveLocation)

def get_cappedHandlesandLabels(ax, data_pivot, max_legend_entries, Columns_Name ):
     # Customize the legend to show up to max_legend_entries entries
     # Sort columns by the sum of values and keep only the top max_legend_entries
     top_columns = data_pivot.sum().nlargest(max_legend_entries).index
     # Add the legend with a limited number of entries
     handles, labels = ax.get_legend_handles_labels()
     outlabels = []
     outhandles = []
     for x in range(len(handles)):
          if labels[x] in top_columns:
               outlabels.append(labels[x])
               outhandles.append(handles[x])

     ax.legend(outhandles, outlabels, title=Columns_Name, loc='best')

     return ax