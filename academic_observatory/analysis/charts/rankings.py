import pandas as pd
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import itertools
from matplotlib import animation, rc
from IPython.display import HTML

from academic_observatory.analysis.charts import (
    AbstractObservatoryChart, regioncolorpalette)
from academic_observatory.analysis.helpers import _collect_kwargs_for


class RankChart(AbstractObservatoryChart):
    """
    Generate a chart in which the rank is on the left and a value is expressed
    on the right to show the relationship between the two.

    Takes a dataframe with columns 'name', a column to generate a rank and a
    column with a value. The rank is generated and then transformed. By default
    rankcol and valcol are the same.
    """

    def __init__(self,
                 df: pd.DataFrame,
                 rankcol: str,
                 filter_name: str,
                 filter_value,
                 rank_length: int = 100,
                 valcol: str = None,
                 colordict: dict = None):
        """Initialisation function

        param: df: pd.DataFrame in the standard COKI format
        param: rankcol: <str> with the name of column containing values to
                        base the ranking on
        param: filter_name: <str> with the name of the column to use for
                            filtering the data, generally a year
        param: filter_value: <str> or <int> value to use for filtering the
                             data to display. Generally the year as an <int>
        param: rank_length: <int> Length of the ranking to compute and plot
        param: valcol: <str> with name of column to use for the values to be
                       plotted against the rank (if different to rankcol)
        param: colordict: <dict> to convert a column to colors for the lines
        """

        super().__init__(df)
        self.rankcol = rankcol
        self.valcol = valcol
        self.filter_name = filter_name
        self.filter_value = filter_value
        self.colordict = colordict
        self.rank_length = rank_length

    def process_data(self, **kwargs):
        """Data selection and processing function

        param: kwargs: Keyword arguments, currently unused

        TODO: Abstraction of the coloring for the error bars
        """
        figdata = self.df
        figdata = figdata[figdata[self.filter_name] == self.filter_value]
        if not self.valcol:
            self.valcol = self.rankcol

        figdata = figdata.sort_values(self.rankcol,
                                      ascending=False)[0:self.rank_length]
        figdata['Rank'] = figdata[self.rankcol].rank(ascending=False)

        # TODO Abstract the coloring
        figdata['color'] = figdata['region'].map(regioncolorpalette)

        if not self.colordict:
            if 'color' in figdata.columns:
                self.colordict = figdata.set_index('name').color.to_dict()
            else:
                self.colordict = {}
        figdata = figdata[['name', 'Rank', self.valcol]].set_index('name')
        figdata = figdata.transpose()
        self.df = figdata
        return self.df

    def plot(self,
             ax: matplotlib.axis = None,
             forcerange=[],
             valaxpad: float = 0,
             show_rank_axis: bool = True,
             rank_axis_distance: float = 1.1,
             scatter: bool = False,
             holes: bool = False,
             line_args: dict = {},
             scatter_args: dict = {},
             hole_args: dict = {},
             **kwargs):
        """Plotting function

        param: ax: matplotlib axis to plot to, default to create new figure
        param: forcerange: two element indexable object providing a low and
                           high value for the right hand spine/axis. Default
                           is an empty list which will use data extent
        param: valaxpad: <float> padding for value axis
        param: rank_axis_distance: <float> distance to displace the rank axis
        param: scatter: <boolean> If true apply jitter to points
        param: holes: <boolean> If true, plot rings rather than dots
        param: line_args: <dict> containing arguments to modify the lines
               plotted
        param: scatter_args: <dict> containing arguments to send to plot method
        param: hole_args: <dict> containing arguments for the holes
        """

        if ax is None:
            left_yaxis = plt.gca()
        else:
            left_yaxis = ax

        # Creating the right axis.
        right_yaxis = left_yaxis.twinx()

        axes = [left_yaxis, right_yaxis]

        # Creating the ranking count axis if show_rank_axis is True
        if show_rank_axis:
            rank_yaxis = left_yaxis.twinx()
            axes.append(rank_yaxis)

        # Sorting the labels to match the ranks.
        left_labels = self.df.iloc[0].sort_values().index
        # right_labels = range(df.iloc[-1].min(), self.df.iloc[-1.max()])

        left_yaxis.set_yticklabels(left_labels)
        if len(forcerange) == 2:
            right_yaxis.set_ylim(*forcerange)
        else:
            right_yaxis.set_ylim(
                self.df.iloc[-1].min()-valaxpad,
                self.df.iloc[-1].max()+valaxpad)

        def scale(y, lines=len(self.df.columns),
                  dataymin=self.df.iloc[-1].min(),
                  dataymax=self.df.iloc[-1].max(),
                  padding=valaxpad,
                  forcerange=forcerange):
            """Function to scale the value column to plot correctly

            TODO: Figure out if this can be done more cleanly with
            matplotlib transform methods.
            """
            if len(forcerange) == 2:
                ymin, ymax = forcerange
            else:
                ymin = dataymin - padding
                ymax = dataymax + padding
            return (0.5 + (ymax-y)/(ymax-ymin)*lines)

        self.df.iloc[1] = self.df.iloc[1].apply(scale)
        for col in self.df.columns:
            y = self.df[col]
            x = self.df.index.values
            # Plotting blank points on the right axis/axes
            # so that they line up with the left axis.
            # for axis in axes[1:]:
            # axis.plot(x, y, alpha= 0)
            if self.df[col].name in self.colordict:
                line_args.update({'color': self.colordict[self.df[col].name]})
            left_yaxis.plot(x, y, **line_args, solid_capstyle='round')

            # Adding scatter plots
            if scatter:
                left_yaxis.scatter(x, y, **scatter_args)

                # Adding see-through holes
                if holes:
                    bg_color = left_yaxis.get_facecolor()
                    left_yaxis.scatter(x, y, color=bg_color, **hole_args)

        # Number of lines
        lines = len(self.df.columns)

        y_ticks = [*range(1, lines + 1)]
        left_yaxis.invert_yaxis()
        left_yaxis.set_yticks(y_ticks)
        left_yaxis.set_ylim((lines+0.5, 0.5))
        left_yaxis.set_xticks([-0.1, 1])

        left_yaxis.spines['left'].set_position(('data', -0.1))
        left_yaxis.spines['top'].set_visible(False)
        left_yaxis.spines['bottom'].set_visible(False)
        left_yaxis.spines['right'].set_visible(False)
        right_yaxis.spines['top'].set_visible(False)
        right_yaxis.spines['bottom'].set_visible(False)
        right_yaxis.spines['left'].set_visible(False)
        right_yaxis.spines['right'].set_position(('data', 1))

        # Setting the position of the far right axis so that
        # it doesn't overlap with the right axis
        if show_rank_axis:
            rank_yaxis.spines["right"].set_position(('data', -0.05))
            rank_yaxis.set_yticks(y_ticks)
            rank_yaxis.set_ylim((lines+0.5, 0.5))
            rank_yaxis.spines['top'].set_visible(False)
            rank_yaxis.spines['bottom'].set_visible(False)
            rank_yaxis.spines['left'].set_visible(False)

        return axes


class ConfidenceIntervalRank(AbstractObservatoryChart):
    """Ranking chart with confidence intervals plotted

    """

    def __init__(self,
                 df: pd.DataFrame,
                 rankcol: str,
                 errorcol: str,
                 filter_name: str,
                 filter_value,
                 rank_length: int = 100,
                 valcol: str = None,
                 colordict: dict = None):
        """
        Initialisation Function

        param: df: pd.DataFrame in the standard COKI format
        param: rankcol: <str> with the name of column containing values to
                        base the ranking on
        param: errorcol: <str> with the name of column containing values for
                         the length of the error bars
        param: filter_name: <str> with the name of the column to use for
                            filtering the data, generally a year
        param: filter_value: <str> or <int> value to use for filtering the
                             data to display. Generally the year as an <int>
        param: rank_length: <int> Length of the ranking to compute and plot
        param: valcol: <str> with name of column to use for the values to be
                       plotted against the rank (if different to rankcol)
        param: colordict: <dict> to convert a column to colors for the lines
        """

        super().__init__(df)
        self.rankcol = rankcol
        self.valcol = valcol
        self.errorcol = errorcol
        self.filter_name = filter_name
        self.filter_value = filter_value
        self.colordict = colordict
        self.rank_length = rank_length

    def process_data(self, **kwargs):
        figdata = self.df
        figdata = figdata[figdata[self.filter_name] == self.filter_value]
        if not self.valcol:
            self.valcol = self.rankcol

        figdata = figdata.sort_values(self.rankcol,
                                      ascending=False)[0:self.rank_length]
        figdata['Rank'] = figdata[self.rankcol].rank(ascending=False)

        # TODO Abstract the coloring
        if 'region' in figdata.columns:
            figdata['color'] = figdata['region'].map(regioncolorpalette)
        else:
            figdata['color'] = 'grey'
        self.df = figdata
        return self.df

    def plot(self,
             ax: matplotlib.axis = None,
             show_rank_axis: bool = True,
             **kwargs):

        if ax is None:
            yaxis = plt.gca()
        else:
            yaxis = ax

        with sns.plotting_context('paper',
                                  rc={'legend.fontsize': 10,
                                      'axes.labelsize': 10},
                                  font_scale=1):
            yaxis.invert_yaxis()
            yaxis.errorbar(self.df[self.rankcol], self.df['name'],
                           xerr=self.df[self.errorcol],
                           fmt='o', ecolor=self.df['color'], elinewidth=4,
                           marker='|', mfc='black', mec='black', ms=8, mew=1.5)
            yaxis.spines['top'].set_visible(False)
            yaxis.spines['right'].set_visible(False)
            yaxis.set_xlabel(f'{self.rankcol}')


class BoxScatter(AbstractObservatoryChart):
    """
    Box and scatter charts for groupings of universitiies
    """

    def __init__(self,
                 df: pd.DataFrame,
                 year: int,
                 group_column: str,
                 plot_column: str,
                 sort: bool = True,
                 sortfunc: callable = pd.DataFrame.median,
                 **kwargs):
        """

        param: df: pd.DataFrame containing data to be plotted
        param: year: int for the year to be plotted
        param: group_column: str giving the column to group by for plot
        param: plot_column: str giving the column to use as values to plot
                            or
                            list giving the set of columns to use
        param: sort: bool default True, whether to sort the groups
        param: sortfunc: pd.DataFrame function, default median
        """

        self.year = year
        self.group_column = group_column
        self.plot_column = plot_column
        self.sort = sort
        self.sortfunc = sortfunc
        super().__init__(df)

    def process_data(self, *kwargs):

        figdata = self.df
        figdata = figdata[figdata.published_year == self.year]
        grouped = figdata.groupby(self.group_column)
        if type(self.plot_column) == list:
            plot_column = self.plot_column[0]
        else:
            plot_column = self.plot_column
            self.plot_column = [self.plot_column]

        if self.sort:
            sort_df = pd.DataFrame(
                {col: vals[plot_column] for col, vals in grouped})
            sfunc = self.sortfunc
            self.group_order = sfunc(sort_df).sort_values(ascending=False)

        self.df = figdata
        return self.df

    def plot(self,
             color: str = 'silver',
             figsize: tuple = (15, 20),
             dodge: bool = False,
             hue: str = 'region',
             xticks: list = [0, 20, 40, 60, 80],
             colorpalette: sns.color_palette = regioncolorpalette,
             alpha: float = 0.5,
             **kwargs):
        """Plot Method

        param: color color for the box plots, must be a color name
                     for matplotlib
        param: figsize tuple with two elements to pass to figsize
        param: dodge sns.boxplot keyword argument for points to avoid
                     overlap
        """

        fig, axes = plt.subplots(1, len(self.plot_column),
                                 figsize=figsize,
                                 sharey=True, sharex=True,
                                 frameon=False)

        if self.sort:
            order = self.group_order.index
        else:
            order = self.df.index
        if len(self.plot_column) > 1:
            panels = zip(axes, self.plot_column)
        else:
            panels = [(axes, self.plot_column[0])]

        for ax, plot_column in panels:
            sns.boxplot(x=plot_column, y=self.group_column,
                        data=self.df, ax=ax,
                        order=order, color="silver", dodge=False)
            sns.swarmplot(x=plot_column, y=self.group_column,
                          data=self.df, ax=ax,
                          order=order, hue='region',
                          palette=colorpalette, alpha=alpha)
            ax.yaxis.label.set_visible(False)
            if xticks:
                ax.set_xticks(xticks)
            ax.get_legend().remove()
        fig.subplots_adjust(wspace=0)
        return fig
