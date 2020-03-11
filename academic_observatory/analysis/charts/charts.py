import pandas as pd
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import itertools
from matplotlib import animation, rc
from IPython.display import HTML
from abc import abstractmethod
from academic_observatory.analysis.resources import AbstractObservatoryResource
from academic_observatory.analysis.helpers import _collect_kwargs_for

regioncolorpalette = {
    'Asia': 'orange',
    'Europe': 'limegreen',
    'North America': 'dodgerblue',
    'Latin America': 'brown',
    'Americas': 'dodgerblue',
    'Africa': 'magenta',
    'Oceania': 'red'
}


class AbstractObservatoryChart(AbstractObservatoryResource):
    """Abstract Base Class for Charts
    """

    def _check_df(self):
        #
        # TODO Some error checking on df being in right form
        return

    @abstractmethod
    def plot(self, ax=None, fig=None, **kwargs):
        """Abstract Plot Method

        All chart classes should have a plot method which draws the
        figure and returns it.
        """
        pass

    @abstractmethod
    def process_data(self):
        pass

    def id2name(self, id):
        return self.df[self.df.grid_id == id].iloc[0]['name']

    def watermark(self,
                  image_file: str,
                  xpad = 0,
                  position: str = 'lower right') -> matplotlib.figure:

        self.fig.set_dpi(300)

        wm_data = matplotlib.image.imread(image_file)
        wm_size_px = wm_data.shape
        # figbounds = self.fig.get_tightbbox(
        #     self.fig.canvas.get_renderer(),
        #     bbox_extra_artists=self.fig.get_children()).corners()

        figsize = self.fig.get_size_inches()
        dpi = self.fig.get_dpi()

        x_displacement = 20
        y_displacement = x_displacement

        x_pos = x_displacement
        y_pos = y_displacement
        if position.endswith('right'):
            x_pos = (figsize[0] * dpi) - x_displacement - wm_size_px[1] - xpad

        if position.startswith('upper'):
            y_pos = (figsize[1] * dpi) - 10 - wm_size_px[0]

        self.fig.figimage(wm_data, x_pos, y_pos, alpha=0.2, origin='upper')
        return self.fig


class ScatterPlot(AbstractObservatoryChart):
    """Scatterplot based on sns.scatterplot for COKI data

    Generates a standard scatter plot with default colors based
    on the region color palette and size of points based on the
    total outputs of the university
    """

    def __init__(self,
                 df: pd.DataFrame,
                 x: str,
                 y: str,
                 filter_name: str,
                 filter_value: str,
                 hue_column: str = 'region',
                 size_column: str = 'total',
                 focus_id: str = None,
                 **kwargs):
        """Initialisation Method
        """

        super().__init__(df)
        self.x = x
        self.y = y
        self.filter_name = filter_name

        if (type(filter_value) == tuple) \
                and (type(filter_value[0]) == int) \
                and (len(filter_value) == 2):
            self.filter_value = range(*filter_value)
        elif type(filter_value) != list:
            self.filter_value = [filter_value]
        else:
            self.filter_value = filter_value

        self.hue_column = hue_column
        self.size_column = size_column
        self.focus_id = focus_id
        self.kwargs = kwargs

    def process_data(self) -> pd.DataFrame:
        """Data processing function

        Currently is hard-coded to sort based on region and
        set an order that works reasonably well for the OA plots.

        TODO Abstract the ordering and colors for better flexibility
        """

        figdata = self.df
        figdata = figdata[figdata[self.filter_name].isin(self.filter_value)]
        sorter = ['Asia', 'Europe', 'North America',
                  'Latin America', 'Africa', 'Oceania']
        sorterIndex = dict(zip(sorter, range(len(sorter))))
        figdata['order'] = figdata.region.map(sorterIndex)
        figdata = figdata.sort_values('order', ascending=True)
        self.df = figdata
        return self.df

    def plot(self,
             ax: matplotlib.axis = None,
             colorpalette: sns.color_palette = None,
             additional_filter=None,
             **kwargs) -> matplotlib.figure:
        """Plot function

        param: ax: The matplotlib axis to plot to
        param: colorpalette: A seaborn or matplotlib color palette used to
                              set the colors for the plot. If the default
                              None is passed, the regioncolorpalette is
                              used.
        param: additional_filter: <tuple> with exactly two elements, one
                                  being the column to filter on and the
                                  other the value. Used for animations.
        param: kwargs: Keyword arguments for sns.relplot,
                       matplotlib plt.subplots and matplotlib ax.set
                       should be picked up and distributed
                       to the appropriate functions.
        """

        scatterplot_kwargs = _collect_kwargs_for(sns.scatterplot, kwargs)
        fig_kwargs = _collect_kwargs_for(plt.figure, kwargs)
        if not ax:
            self.fig, self.ax = plt.subplots(**fig_kwargs)

        if not colorpalette:
            colorpalette = regioncolorpalette

        figdata = self.df
        if additional_filter:
            col, value = additional_filter
            figdata = figdata[figdata[col] == value]

        sns.scatterplot(x=self.x, y=self.y, data=figdata,
                        size=self.size_column, sizes=(50, 600),
                        hue=self.hue_column, alpha=0.6,
                        palette=colorpalette,
                        ax=self.ax,
                        **scatterplot_kwargs)
        if self.focus_id:
            sns.scatterplot(x=self.x, y=self.y,
                            data=figdata[figdata.grid_id == self.focus_id],
                            color="black", s=500, marker='X', legend=False,
                            ax=self.ax)
        self.ax.spines['top'].set_visible(False)
        self.ax.spines['right'].set_visible(False)
        self.ax.legend(loc='upper right', bbox_to_anchor=(1.1, 0.8))
        self.ax.set(**kwargs)
        return self.fig

    def animate(self,
                colorpalette: sns.color_palette = None,
                year_range=None,
                numframes: int = None,
                frameinterval: int = 1000,
                **kwargs):
        """User animate function for scatterplot

        param: colorpalette: matplotlib colorpalette, default None
        param: year_range: optional, defaults to using self.filter_value
                           <tuple> with exactly two or three
                           elements which is passed to range to generate
                           list of years or
                           <list> of years which will be used directly
        param: numframes: optional <int> to set number of frames, defaults
                          to the length of the year_range plus five to
                          pause at the end
        param: frameinterval: optional <int> to set the frame rate of
                              the animation in milliseconds, defaults to
                              one frame per second
        param: kwargs: kwargs are collected for figure, scatterplot and
                       the remainder sent to ax.set()

        returns: HTML5 video representation of the animation

        TODO Generalise the output form to allow for JS and other
        representations of the animation.
        """

        fig_kwargs = _collect_kwargs_for(plt.figure, kwargs)
        self.plot_kwargs = kwargs
        self.color_palette = colorpalette

        if year_range:
            if type(year_range) == tuple:
                self.year_range = range(*year_range)
            elif type(year_range) == list:
                self.year_range == year_range
        elif not year_range:
            self.year_range = self.filter_value

        if not numframes:
            numframes = len(self.year_range) + 5

        self.fig, self.ax = plt.subplots(**fig_kwargs)

        self.anim = animation.FuncAnimation(self.fig,
                                            self.anim_frame,
                                            numframes,
                                            interval=frameinterval)
        return HTML(self.anim.to_html5_video())

    def anim_frame(self, i: int):
        """Frame animation function for scatterplot

        param: i: framenumber
        """

        year = self.year_range[0] + i + 1
        if year in self.year_range:
            yearstring = str(year)
        else:
            year = self.year_range[-1]
            yearstring = str(year)

        self.ax.clear()
        self.plot(ax=self.ax,
                  colorpalette=self.color_palette,
                  additional_filter=('published_year', year),
                  **self.plot_kwargs)

        self.ax.text(0.05, 0.95,
                     yearstring,
                     transform=self.ax.transAxes,
                     fontsize=14,
                     verticalalignment='top')
        plt.close()


class TimePlot(AbstractObservatoryChart):
    """Line charts for showing points of change in time
    """

    def __init__(self, df, year_range, unis, plot_column,
                 hue_column='name',
                 size_column=None,
                 **kwargs):
        """Init Function

        param: year_range: tuple with two elements for range of years to plot
        param: unis: list of grid_ids to include
        param: plot_column: name of column of input df to use as values
        return: None
        """

        self.year_range = range(*year_range)
        self.unis = unis
        self.plot_column = plot_column
        self.hue_column = hue_column
        self.size_column = size_column
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
                                color='black',
                                title=[n for n in self.df.columns])

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

        if marker_line:
            [ax.axvline(marker_line, 0, 1.2, color='grey',
                        linestyle='dashed', clip_on=False) for ax in axes]

        return fig


class TimePlotLayout(AbstractObservatoryChart):
    """Layout made up of TimePlots
    """

    def __init__(self, df,
                 plots,
                 **kwargs):
        """Init function

        param: df: pd.DataFrame in COKI standard format
        param: plots: a list of dicts, each of which must
                       conform to the following structure:

{
    year_range: (2010, 2018),
            # A tuple with two elements containing a start and end year
    y_column: 'Total Gold OA (%)
            # A str containing a column name with y values
    unis: ['id1', 'id2', 'id3']
            # An ordered list of identifiers for plotting
}
        """

        self.df = df
        assert type(plots) == list
        for plot in [p for p in plots]:
            assert type(plot) == dict
            for k in ['year_range', 'y_column', 'unis']:
                assert k in plot
        self.plots = plots
        self.kwargs = kwargs
        super().__init__(df)

    def process_data(self, **kwargs):
        self.plot_data = [None for _ in range(len(self.plots))]
        for i, plot in enumerate(self.plots):
            year_range = plot.get('year_range')
            years = range(*year_range)
            self.plot_data[i] = self.df[
                self.df.published_year.isin(years) &
                self.df.grid_id.isin(plot.get('unis'))
            ].sort_values('published_year')

    def plot(self, fig=None,
             ylabel_adjustment=0.025,
             panel_labels=False,
             panellable_adjustment=0.01,
             **kwargs):

        figure_kwargs = {k: kwargs[k] for k in kwargs.keys() &
                         {'figsize', 'sharey', 'sharex'}}

        gridspec_kwargs = {k: kwargs[k] for k in kwargs.keys() &
                           {'wspace', 'hspace'}}
        if not fig:
            fig = plt.figure(**figure_kwargs)
        layout = fig.add_gridspec(1, len(self.plots), **gridspec_kwargs)

        for i, plot in enumerate(self.plots):
            subspec = layout[i].subgridspec(len(plot.get('unis')), 1)
            for j, uni in enumerate(plot.get('unis')):
                ax = fig.add_subplot(subspec[j])
                ax_df = self.plot_data[i]
                ax_data = ax_df[ax_df.grid_id == uni]
                ax_data.plot(x='published_year', y=plot.get('y_column'),
                             ax=ax, legend=False, title=self.id2name(uni))
                if plot.get('markerline'):
                    if ax.is_first_row():
                        ax.axvline(plot.get('markerline'),
                                   0, 1, color='grey',
                                   linestyle='dashed', clip_on=False)
                    else:
                        ax.axvline(plot.get('markerline'),
                                   0, 1.2, color='grey',
                                   linestyle='dashed', clip_on=False)
                ax.set(**_collect_kwargs_for(ax.set, plot))

        all_axes = fig.get_axes()

        for ax in all_axes:
            for sp in ax.spines.values():
                sp.set_visible(False)

            ax.get_xaxis().set_visible(False)
            ax.spines['left'].set_visible(True)

            ax.title.set_ha('left')
            ax.title.set_position([0.03, 0.95])
            if ax.is_last_row():
                ax.spines['bottom'].set_visible(True)
                ax.get_xaxis().set_visible(True)

        subplots_params = []
        for i in range(len(self.plots)):
            subplots_params.append(layout[i].get_position(fig))

        for i, plot in enumerate(self.plots):
            ylabel = plot.get('y_column')
            xpos = subplots_params[i].x0
            ypos = subplots_params[i].y1
            fig.text(xpos - ylabel_adjustment, 0.5, ylabel,
                     ha='center', va='center', rotation='vertical')
            if panel_labels:
                labels = ['A', 'B', 'C', 'D', 'E', 'F']
                fig.text(xpos, ypos + panellable_adjustment,
                         labels[i], fontsize='xx-large', fontweight='bold')

        return fig


class TimePath(AbstractObservatoryChart):
    """Charts to illustrate movement over time in two dimensions
    """

    def __init__(self, df, year_range, unis, x, y,
                 hue_column='name',
                 size_column=None,
                 **kwargs):
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
        self.hue_column = hue_column
        self.size_column = size_column
        super().__init__(df)

    def process_data(self, **kwargs):
        figdata = self.df
        for uni in self.unis:
            try:
                assert uni in figdata['grid_id'].values
            except AssertionError:
                print(uni, 'not in list of ids')
        figdata = figdata[(figdata.grid_id.isin(self.unis)) &
                          figdata.published_year.isin(self.year_range)]
        figdata['order'] = figdata['grid_id'].map(
            lambda v: self.unis.index(v))
        figdata = figdata.sort_values(
            ['order', 'published_year'], ascending=True)
        self.df = figdata
        return self.df

    def plot(self, year_range=None, colorpalette=None, ax=None, **kwargs):
        if not year_range:
            year_range = self.year_range
        if not colorpalette:
            colorpalette = sns.color_palette("husl", len(self.unis))

        if not ax:
            figsize = kwargs.pop('figsize', None)
            fig, ax = plt.subplots(figsize=figsize)

        figdata = self.df[self.df.published_year.isin(year_range)]

        sns.scatterplot(x=self.xcolumn, y=self.ycolumn,
                        data=figdata, s=20,
                        hue=self.hue_column, ax=ax, palette=colorpalette)
        sns.lineplot(x=self.xcolumn, y=self.ycolumn,
                     data=figdata, sort=False, legend=False,
                     hue=self.hue_column, ax=ax, palette=colorpalette)

        head_width = kwargs.pop('arrow_width', None)
        if not head_width:
            head_width = 2

        if len(year_range) > 1:
            for i, uni in enumerate(self.unis):
                x = figdata[
                    (figdata.grid_id == uni) &
                    (figdata.published_year == year_range[-2])
                ][self.xcolumn].iloc[0]
                y = figdata[
                    (figdata.grid_id == uni) &
                    (figdata.published_year == year_range[-2])
                ][self.ycolumn].iloc[0]
                dx = figdata[
                    (figdata.grid_id == uni) &
                    (figdata.published_year == year_range[-1])
                ][self.xcolumn].iloc[0] - x
                dy = figdata[
                    (figdata.grid_id == uni) &
                    (figdata.published_year == year_range[-1])
                ][self.ycolumn].iloc[0] - y
                if type(colorpalette) == list:
                    color = colorpalette[i]
                elif type(colorpalette) == dict:
                    _, color = colorpalette.items()[i]
                ax.arrow(x, y, dx, dy, color=color, head_width=head_width)

        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.legend(loc='upper right', bbox_to_anchor=(1.1, 0.8))
        ax.set(**kwargs)
        # return fig

    def animate(self, colorpalette=None, year_range=None, **kwargs):
        self.plot_kwargs = kwargs
        self.color_palette = colorpalette
        if not year_range:
            year_range = self.year_range

        figsize = kwargs.pop('figsize', None)
        fig, self.ax = plt.subplots(figsize=figsize)

        self.anim = animation.FuncAnimation(fig, self.anim_frame,
                                            (len(year_range)+5), interval=1000)

        return HTML(self.anim.to_html5_video())

    def anim_frame(self, i):
        self.ax.clear()
        self.plot(self.year_range[0:i+2], colorpalette=self.color_palette,
                  ax=self.ax, **self.plot_kwargs)
        year = self.year_range[0] + i + 1
        if year in self.year_range:
            yearstring = f'{self.year_range[0]} - {year}'
        else:
            yearstring = f'{self.year_range[0]} - {self.year_range[-1]}'
        self.ax.text(0.05, 0.95,
                     yearstring,
                     transform=self.ax.transAxes,
                     fontsize=14,
                     verticalalignment='top')
        plt.close()


class Layout(AbstractObservatoryChart):
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
