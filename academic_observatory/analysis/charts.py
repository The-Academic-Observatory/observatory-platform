import pandas as pd
import pandas_gbq
import seaborn as sns
import matplotlib.pyplot as plt
import itertools
from matplotlib import animation, rc
from IPython.display import HTML

regioncolorpalette = {
    'Asia': 'orange',
    'Europe': 'limegreen',
    'North America': 'dodgerblue',
    'Latin America': 'brown',
    'Africa': 'magenta',
    'Oceania': 'red'
}


class AbstractCokiChart():
    """Abstract Class for COKI Charts"""

    def __init__(self, df):
        self.df = df

    def _check_df(self):
        #
        # TODO Some error checking on df being in right form
        return

    def save_image(self, name, kwargs):
        raise NotImplementedError

    def save_gcp_bucket(self, name, bucket, kwargs):
        raise NotImplementedError


class StaticScatterPlot(AbstractCokiChart):

    def __init__(self, df, x, y, filter_name, filter_value, **kwargs):
        super().__init__(df)
        self.x = x
        self.y = y
        self.filter_name = filter_name
        self.filter_value = filter_value
        self.kwargs = kwargs

    def process_data(self):
        figdata = self.df
        figdata = figdata[figdata[self.filter_name] == self.filter_value]
        sorter = ['Asia', 'Europe', 'North America',
                  'Latin America', 'Africa', 'Oceania']
        sorterIndex = dict(zip(sorter, range(len(sorter))))
        figdata['order'] = figdata['region'].map(sorterIndex)
        figdata = figdata.sort_values('order', ascending=True)
        self.df = figdata
        return self.df

    def plot(self, **kwargs):
        p = sns.relplot(x=self.x, y=self.y, data=self.df,
                        size='total', sizes=(50, 600),
                        hue='region', alpha=0.6,
                        palette=regioncolorpalette)
        p = p.set(**kwargs)
        return p


class RankChart(AbstractCokiChart):
    """
    Generate a chart in which the rank is on the left and a value is expressed
    on the right to show the relationship between the two.

    Takes a dataframe with columns 'name', a column to generate a rank and a
    column with a value. The rank is generated and then transformed. By default
    rankcol and valcol are the same.
    """

    def __init__(self, df, rankcol, filter_name, filter_value,
                 rank_length=100,
                 valcol=None, colordict=None):
        super().__init__(df)
        self.rankcol = rankcol
        self.valcol = valcol
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
             ax=None, forcerange=[], valaxpad=0,
             show_rank_axis=True, rank_axis_distance=1.1,
             scatter=False, holes=False,
             line_args={}, scatter_args={}, hole_args={},
             **kwargs):
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
            """Function to scale the value column to plot correctly"""
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


class ConfidenceIntervalRank(AbstractCokiChart):
    """Ranking chart with confidence intervals plotted

    """

    def __init__(self, df, rankcol, errorcol,
                 filter_name, filter_value,
                 rank_length=100,
                 valcol=None, colordict=None):
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
        figdata['color'] = figdata['region'].map(regioncolorpalette)
        self.df = figdata
        return self.df

    def plot(self,
             ax=None,
             show_rank_axis=True,
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


class BoxScatter(AbstractCokiChart):
    """
    Box and scatter charts for groupings of universitiies
    """

    def __init__(self, df, year,
                 group_column, plot_column,
                 sort=True, sortfunc=pd.DataFrame.median,
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
             color='silver',
             figsize=(15, 20),
             dodge=False,
             hue='region',
             xticks=[0, 20, 40, 60, 80],
             colorpalette=regioncolorpalette,
             alpha=0.5,
             **kwargs):

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
            ax.set_xticks(xticks)
            ax.get_legend().remove()
        fig.subplots_adjust(wspace=0)


class TimePlot(AbstractCokiChart):
    """Line charts for showing points of change in time
    """

    def __init__(self, df, year_range, unis, plot_column, **kwargs):
        """Init Function

        param: year_range: tuple with two elements for range of years to plot
        param: unis: list of grid_ids to include
        param: plot_column: name of column of input df to use as values
        return: None
        """

        self.year_range = range(*year_range)
        self.unis = unis
        self.plot_column = plot_column
        super().__init__(df)

    def process_data(self, *kwargs):
        figdata = self.df
        columnorder = [figdata[figdata.grid_id == grid].iloc[0]['name']
                       for grid in self.unis]
        figdata = figdata[(figdata.published_year.isin(
            self.year_range)) & (figdata.grid_id.isin(self.unis))]
        figdata = figdata.pivot(index='published_year',
                                columns="name", values=self.plot_column)
        figdata = figdata.reindex(columnorder, axis=1)
        self.df = figdata
        return self.df

    def plot(self, ax=None, xticks=None, marker_line=None,
             ylim=None, **kwargs):

        plot_kwargs = {k: kwargs[k] for k in kwargs.keys() &
                       {'figsize', 'sharey', 'sharex', 'frameon'}}
        if not ax:
            fig, axes = plt.subplots(len(self.unis), 1, sharex=True,
                                     frameon=False, **plot_kwargs)
            self.df.plot(subplots=True, ax=axes, legend=False,
                         color='black', title=[n for n in self.df.columns])

        else:
            axes = self.df.plot(subplots=True, ax=ax, legend=False,
                                color='black', title=[n for n in self.df.columns])

        [ax.spines[loc].set_visible(False) for ax, loc in itertools.product(
            axes, ['top', 'right', 'bottom'])]
        [ax.tick_params(axis='x', which='both', bottom=False, top=False,
                        labelbottom=False) for ax in axes[0:len(self.unis)-1]]
        if ylim:
            if len(ylim) == 2:
                b, t = ylim
                [ax.set_ylim(bottom=b, top=t) for ax in axes[0:len(self.unis)]]
            else:
                [ax.set_ylim(bottom=ylim) for ax in axes[0:len(self.unis)]]
        [ax.title.set_ha('left') for ax in axes[0:len(self.unis)]]
        [ax.title.set_position([0.03, 0.5]) for ax in axes[0:len(self.unis)]]

        axes[-1].spines['bottom'].set_visible(True)
        if xticks:
            axes[-1].set_xticks(xticks)
        axes[-1].tick_params(axis='x', which='minor', bottom=False)

        if marker_line in kwargs:
            [ax.axvline(marker_line, 0, 1.2, color='grey',
                        linestyle='dashed', clip_on=False) for ax in axes]


class TimePath(AbstractCokiChart):
    """Charts to illustrate movement over time in two dimensions
    """

    def __init__(self, df, year_range, unis, x, y, **kwargs):
        """
        param: df: input data frame
        param: year_range: duple containing first and last+1 year
        param: unis: a list of ids
        param: x: str, column name for x values
        param: y: str, column name for y values
        """

        self.xcolumn = x
        self.ycolumn = y
        self.year_range = range(*year_range)
        self.unis = unis
        super().__init__(df)

    def process_data(self, **kwargs):
        figdata = self.df
        figdata = figdata[(figdata.grid_id.isin(self.unis)) &
                          figdata.published_year.isin(self.year_range)]
        figdata['order'] = figdata['grid_id'].map(
            lambda v: self.unis.index(v))
        figdata = figdata.sort_values(
            ['order', 'published_year'], ascending=True)
        self.df = figdata
        return self.df

    def plot(self, colorpalette=None, **kwargs):
        if not colorpalette:
            colorpalette = sns.color_palette()
            colorpalette = colorpalette[:len(self.unis)]

        p = sns.relplot(x=self.xcolumn, y=self.ycolumn,
                    data=self.df, s=20,
                    hue='name', palette=colorpalette)
        sns.lineplot(x=self.xcolumn, y=self.ycolumn,
                     data=self.df, sort=False, legend=False,
                     hue='name', palette=colorpalette)
        for i, uni in enumerate(self.unis):
            x = self.df[
                (self.df.grid_id == uni) &
                (self.df.published_year == self.year_range[-2])
            ][self.xcolumn].iloc[0]
            y = self.df[
                (self.df.grid_id == uni) &
                (self.df.published_year == self.year_range[-2])
            ][self.ycolumn].iloc[0]
            dx = self.df[
                (self.df.grid_id == uni) &
                (self.df.published_year == self.year_range[-1])
            ][self.xcolumn].iloc[0] - x
            dy = self.df[
                (self.df.grid_id == uni) &
                (self.df.published_year == self.year_range[-1])
            ][self.ycolumn].iloc[0] - y
            color = colorpalette[i]
            plt.arrow(x, y, dx, dy, color=color, head_width=2)

        p = p.set(**kwargs)
        return p

class Layout(AbstractCokiChart):
    """General Class for handling multi-chart layouts


    """

    def __init__(self, df, charts):
        """
        :param df: A data frame conforming to the COKI table format
        :param charts: A list of dictionaries containing the
               initiatialisation params and kwargs for the sub-charts
        :return: A figure with the relevant charts as subplots
        """

        self.chart_params = charts
        self.charts = []
        super().__init__(df)

    def process_data(self):
        for params in self.chart_params:
            params['df'] = self.df
            chart_class = params.pop('chart_class')
            chart = chart_class(**params)
            self.charts.append(chart)

        for chart in self.charts:
            chart.process_data()

    def plot(self,
             figsize=(15, 20),
             **kwargs):
        fig, axes = plt.subplots(1, len(self.charts),
                                 figsize=figsize,
                                 sharey=False,
                                 sharex=False,
                                 frameon=False)

        for chart, ax in zip(self.charts, axes):
            chart.plot(ax=ax, **kwargs)

        if 'wspace' in kwargs:
            fig.subplots_adjust(wspace=kwargs['wspace'])

        return fig


def _coki_standard_format(style='seaborn-white',
                          context='paper'):
    """Convenience function for defining the COKI standard formats for plots"""

    plt.style.use(style)
    sns.set_style('ticks')
    sns.set_context(context)
