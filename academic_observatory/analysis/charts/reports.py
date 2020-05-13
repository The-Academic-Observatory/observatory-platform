import pandas as pd
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import itertools
from matplotlib import animation, rc
from IPython.display import HTML
from academic_observatory.analysis.charts import (
    AbstractObservatoryChart)
from academic_observatory.analysis import helpers, defaults


class OutputTypesPieChart(AbstractObservatoryChart):
    """Generate a Pie Graph based on Output Types
    """

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 focus_year: int):
        self.df = df
        self.focus_year = focus_year
        self.identifier = identifier

    def process_data(self):
        type_categories = defaults.output_types
        figdata = self.df[(self.df.id == self.identifier) &
                          (self.df.published_year == self.focus_year) &
                          (self.df.type.isin(type_categories))
                          ][['type', 'total']]
        figdata['type_category'] = pd.Categorical(figdata.type,
                                                  categories=defaults.output_types,
                                                  ordered=True)
        figdata = figdata.set_index('type_category')
        self.figdata = figdata
        return self.figdata

    def plot(self, ax=None, **kwargs):
        palette = [defaults.outputs_palette[k] for k in defaults.output_types]
        outputs_pie = self.figdata.plot.pie(y='total',
                                            startangle=90,
                                            labels=None,
                                            legend=True,
                                            colors=palette,
                                            ax=ax)
        outputs_pie.set_ylabel('')
        my_circle = plt.Circle((0, 0), 0.4, color='white')
        p = plt.gcf()
        p.gca().add_artist(my_circle)
        outputs_pie.legend(labels=defaults.output_types,
                           bbox_to_anchor=(1, 0.8))
        self.fig = outputs_pie.get_figure()
        return self.fig


class GenericTimeChart(AbstractObservatoryChart):
    """Generic Graph of values over time by publication year
    """

    def __init__(self,
                 df: pd.DataFrame,
                 columns: list,
                 identifier: str,
                 year_range: tuple = (2005, 2020)):
        self.df = df
        self.year_range = year_range
        self.columns = columns
        self.identifier = identifier
        self.melt_var_name = 'variable'

    def process_data(self):
        columns = ['id', 'Year of Publication'] + self.columns
        figdata = self.df[columns]
        figdata = self.df.melt(
            id_vars=['id',
                     'Year of Publication'],
            var_name=self.melt_var_name
        )
        figdata = figdata[
            (figdata[self.melt_var_name].isin(self.columns)) &
            (figdata.id == self.identifier) &
            (figdata['Year of Publication'].isin(range(*self.year_range)))
        ]
        figdata.value = figdata.value.astype('float64')

        figdata.sort_values('Year of Publication', inplace=True)
        self.figdata = figdata
        return self.figdata

    def plot(self, palette=None, ax=None, lines=None, **kwargs):
        if not palette:
            palette = sns.color_palette('husl', n_colors=len(self.columns))
        if not ax:
            self.fig, ax = plt.subplots(figsize=(5, 5))
        sns.lineplot(x='Year of Publication',
                     y='value',
                     data=self.figdata,
                     hue=self.melt_var_name,
                     hue_order=self.columns,
                     marker='o',
                     palette=palette,
                     ax=ax)
        ax.legend(bbox_to_anchor=(1, 0.8))
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        if lines:
            ax.axhline(**lines)
        return self.fig


class OutputTypesTimeChart(GenericTimeChart):
    """Generate a Plot of Output Types Over Time

    Shares the `types_palette` with OutputTypesPieChart
    """

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 year_range: tuple = (2000, 2020)
                 ):
        columns = ['Output Types', 'value']
        super().__init__(df, columns, identifier, year_range)
        self.melt_var_name = 'Output Types'

    def process_data(self):
        self.df['Output Types'] = self.df.type
        self.df['value'] = self.df.total
        columns = ['id', 'Year of Publication'] + self.columns
        self.columns = defaults.output_types
        figdata = self.df[self.df.id == self.identifier][columns]
        figdata.sort_values('Year of Publication', inplace=True)
        self.figdata = figdata
        return self.figdata

    def plot(self, palette=defaults.outputs_palette,
             ax=None, **kwargs):
        self.fig = super().plot(palette=defaults.outputs_palette,
                                ax=ax, **kwargs)
        return self.fig


class OApcTimeChart(GenericTimeChart):
    """Generate a Plot of Standard OA Types Over Time

    Produces a standard line plot coloured by OA type for
    a single id
    """

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 year_range: tuple = (2000, 2020),
                 ):

        columns = defaults.oa_types
        super().__init__(df, columns, identifier, year_range)
        self.melt_var_name = 'Access Type'

    def plot(self, palette=defaults.oatypes_palette,
             ax=None, **kwargs):
        self.fig = super().plot(palette, ax=ax, **kwargs)
        return self.fig


class CitationCountTimeChart(GenericTimeChart):
    """Generates a plot of Citation Counts per Published Year
    """

    def __init__(self,
                 df: pd.DataFrame,
                 identifier: str,
                 year_range: tuple = (2000, 2020),
                 chart_type: str = 'count'):

        self.chart_type = chart_type
        columns = [
            'Citation Count',
            'Cited Articles',
            'Citations to OA Outputs',
            'Citations to Gold Outputs',
            'Citations to Green Outputs',
            'Citations to Hybrid Outputs'
        ]
        super().__init__(df, columns, identifier, year_range)

    def process_data(self):
        if self.chart_type in ['per-article', 'advantage']:
            self.df['Non-OA'] = (self.df.total_citations - self.df.oa_citations
                                 ) / (self.df.total - self.df.total_oa)
            self.df['All Outputs'] = self.df.total_citations / self.df.total
            self.df['Open Access'] = self.df.oa_citations / self.df.total_oa
            self.df['Gold OA'] = self.df.gold_citations / self.df.gold
            self.df['Green OA'] = self.df.green_citations / self.df.green
            self.df['Hybrid OA'] = self.df.hybrid_citations / self.df.hybrid

            if self.chart_type == 'advantage':
                self.df['Non-Open Access'] = self.df['Non-OA'] /   \
                    self.df['All Outputs']
                self.df['Open Access'] = self.df['Open Access'] /  \
                    self.df['All Outputs']
                self.df['Gold OA'] = self.df['Gold OA'] /          \
                    self.df['All Outputs']
                self.df['Green OA'] = self.df['Green OA'] /        \
                    self.df['All Outputs']
                self.df['Hybrid OA'] = self.df['Hybrid OA'] /      \
                    self.df['All Outputs']
                self.columns = ['Non-Open Access',
                                'Open Access',
                                'Gold OA',
                                'Green OA',
                                'Hybrid OA']
                self.melt_var_name = 'Citation Advantage'

            elif self.chart_type == 'per-article':
                self.columns = ['All Outputs',
                                'Open Access',
                                'Gold OA',
                                'Green OA',
                                'Hybrid OA']
                self.melt_var_name = 'Citations per Output'
        self.figdata = super().process_data()
        return self.figdata

    def plot(self, palette=None, ax=None, **kwargs):
        if not palette:
            if self.chart_type == 'count':
                palette = ['blue', 'orange',
                           'black', 'gold', 'darkgreen', 'orange']
            if self.chart_type in ['per-article', 'advantage']:
                palette = ['red', 'black', 'gold', 'darkgreen', 'orange']
        if self.chart_type == 'advantage':
            lines = {'y': 1,
                     'xmin': 0,
                     'xmax': 0,
                     'color': 'grey',
                     'linestyle': 'dashed'}
        else:
            lines = None

        self.fig = super().plot(palette, ax=ax, lines=lines, **kwargs)
        return self.fig


class OAAdvantageBarChart(AbstractObservatoryChart):
    """Generates a bar chart of the OA citation advantage for a year
    """

    def __init__(self,
                 df: pd.DataFrame,
                 focus_year: int,
                 identifier: str):
        self.df = df
        self.focus_year = focus_year
        self.identifier = identifier

    def process_data(self):
        self.df['Citations per non-OA Output'] = (self.df.total_citations -
                                                  self.df.oa_citations
                                                  ) / (self.df.total -
                                                       self.df.total_oa)
        self.df['Citations per Output (all)'] = self.df.total_citations /     \
            self.df.total
        self.df['Citations per OA Output'] = self.df.oa_citations /           \
            self.df.total_oa
        self.df['Citations per Gold OA Output'] = self.df.gold_citations /    \
            self.df.gold
        self.df['Citations per Green OA Output'] = self.df.green_citations /  \
            self.df.green
        self.df['Citations per Hybrid OA Output'] = self.df.hybrid_citations /\
            self.df.hybrid
        self.df['Non-Open Access'] = self.df['Citations per non-OA Output'] / \
            self.df['Citations per Output (all)']
        self.df['Open Access'] = self.df['Citations per OA Output'] /         \
            self.df['Citations per Output (all)']
        self.df['Gold OA'] = self.df['Citations per Gold OA Output'] /        \
            self.df['Citations per Output (all)']
        self.df['Green OA'] = self.df['Citations per Green OA Output'] /      \
            self.df['Citations per Output (all)']
        self.df['Hybrid OA'] = self.df['Citations per Hybrid OA Output'] /    \
            self.df['Citations per Output (all)']
        self.columns = ['Non-Open Access',
                        'Open Access',
                        'Gold OA',
                        'Green OA',
                        'Hybrid OA']
        figdata = self.df[(self.df.id == self.identifier) &
                          (self.df.published_year == self.focus_year)
                          ][self.columns]
        figdata = figdata.melt()
        figdata.set_index('variable', inplace=True)
        self.figdata = figdata
        return self.figdata

    def plot(self, ax=None, **kwargs):
        if not ax:
            self.fig, ax = plt.subplots()
        self.figdata.plot(kind='bar',
                          # color=['red', 'black', 'gold',
                          #       'darkgreen', 'orange'],
                          ax=ax)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        for label in ax.get_xticklabels():
            label.set_rotation(30)
            label.set_size(12)
            label.set_ha('right')
        ax.set(ylabel='Advantage (Times)', xlabel='Access Type')
        ax.axhline(1, 0, 1, color='grey', linestyle='dashed')
        ax.legend_.remove()
        return self.fig


class BarComparisonChart(AbstractObservatoryChart):
    """Generates BarPlot of OA outputs with helpful defaults

    Produces a standard bar plot with appropriate colors for
    Bronze, Hybrid, DOAJ_Gold and Green OA.

    TODO Greater flexibility for color palettes and options
    for other variations on OA.
    """

    def __init__(self,
                 df: pd.DataFrame,
                 comparison: list,
                 focus_year: int,
                 color_palette=['brown', 'orange', 'gold', 'green']
                 ):
        """Initialisation function

        param: df: pd.DataFrame in the standard COKI format
        param: comparison: <list> of grid IDs
        param: year: <int> year for the data to be used to plot
        param: color_palette: matplotlib color palette, default colors
                              used for an OA types contribution bar plot
        """

        self.comparison = comparison
        self.focus_year = focus_year
        self.color_palette = color_palette
        self.df = df

    def process_data(self, **kwargs):
        """Data selection and processing function

        param: kwargs: Keyword arguments, currently unused

        TODO: Current hardcodes the location of uni ids
        and the year in the dataframe. Generalise this and allow
        for more flexibility of data types
        """

        figdata = self.df[(self.df.published_year == self.focus_year) &
                          (self.df.id.isin(self.comparison))]
        figdata = figdata.set_index('id').reindex(self.comparison)
        figdata = figdata.set_index('name')
        self.figdata = figdata
        return self.figdata

    def plot(self, ax=None, **kwargs):
        """Plotting function

        param: kwargs: Any keywords to be sent to plt.figure or ax.set
                       during the plotting process.

        TODO: Currently hardcodes the kinds of OA to be plotted. This should
        be abstracted to allow greater flexibility.
        """

        if not ax:
            self.fig, ax = plt.subplots()
        self.figdata[['Bronze (%)',
                      'Hybrid OA (%)',
                      'Gold in DOAJ (%)',
                      'Green Only (%)']].plot(
            kind='bar', stacked=True, colors=self.color_palette, ax=ax)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        for label in ax.get_xticklabels():
            label.set_rotation(30)
            label.set_size(12)
            label.set_ha('right')

        ax.set(ylabel='Percent of all outputs', xlabel=None)
        ax.legend(bbox_to_anchor=(1, 0.8))
        return self.fig


class FunderGraph(AbstractObservatoryChart):
    """Two part figure showing OA by funder
    """

    def __init__(self,
                 df: pd.DataFrame,
                 focus_year: int,
                 num_funders: int = 10,
                 identifier: str = None):

        self.focus_year = focus_year
        self.num_funders = num_funders
        self.identifier = identifier
        super().__init__(df)

    def process_data(self):
        data = self.df[self.df.published_year == self.focus_year]
        if self.identifier:
            data = data[data.id == self.identifier]
        data = data.sort_values('count', ascending=False)[0:self.num_funders]

        data = data.melt(id_vars=['published_year', 'name'],
                         var_name='variables')
        self.figdata = data
        return self.figdata

    def plot(self):
        self.fig, axes = plt.subplots(
            nrows=1, ncols=2, sharey=True, figsize=(8, 4))
        sns.catplot(y="name",
                    x="value",
                    hue="variables",
                    kind="bar",
                    data=self.figdata[self.figdata.variables.isin(['count',
                                                                   'oa'])],
                    ax=axes[0])
        axes[0].set(ylabel=None, xlabel='Number of Outputs')
        handles, labels = axes[0].get_legend_handles_labels()
        axes[0].legend(handles, ['Total', 'Open Access'])
        sns.catplot(y="name",
                    x="value",
                    kind="bar",
                    data=self.figdata[self.figdata.variables == 'percent_oa'],
                    color='blue', ax=axes[1])
        axes[1].set(ylabel=None, xlabel='% Open Access')
        self.fig.set_dpi(300)
        return self.fig
