import pandas as pd
import pandas_gbq
import seaborn as sns
import matplotlib.pyplot as plt
import itertools
from matplotlib import animation, rc
from IPython.display import HTML

colorpalette = {
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
                        hue='region', alpha=0.6, height=8,
                        palette=colorpalette)
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
                 valcol=None, valaxpad=1,
                 colordict=None, forcerange=[],
                 show_rank_axis=True, rank_axis_distance=1.1,
                 ax=None, scatter=False, holes=False,
                 line_args={}, scatter_args={}, hole_args={}):
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
        figdata['color'] = figdata.apply(
            lambda row: colorpalette[row.region], axis=1)

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


def _coki_standard_format(style='seaborn-white',
                          context='paper'):
    """Convenience function for defining the COKI standard formats for plots"""

    plt.style.use(style)
    sns.set_style('ticks')
    sns.set_context(context)
