# MIT License
#
# Copyright (c) 2017 Feng Wang
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import matplotlib.patches as patches
import numpy as np
from matplotlib.path import Path

LW = 0.3


def polar2xy(r, theta):
    return np.array([r * np.cos(theta), r * np.sin(theta)])


def hex2rgb(c):
    return tuple(int(c[i:i + 2], 16) / 256.0 for i in (1, 3, 5))


def ideogram_arc(start=0, end=60, radius=1.0, width=0.2, ax=None, color=(1, 0, 0)):
    # start, end should be in [0, 360)
    if start > end:
        start, end = end, start
    start *= np.pi / 180.
    end *= np.pi / 180.
    # optimal distance to the control points
    # https://stackoverflow.com/questions/1734745/how-to-create-circle-with-b%C3%A9zier-curves
    opt = 4. / 3. * np.tan((end - start) / 4.) * radius
    inner = radius * (1 - width)
    verts = [
        polar2xy(radius, start),
        polar2xy(radius, start) + polar2xy(opt, start + 0.5 * np.pi),
        polar2xy(radius, end) + polar2xy(opt, end - 0.5 * np.pi),
        polar2xy(radius, end),
        polar2xy(inner, end),
        polar2xy(inner, end) + polar2xy(opt * (1 - width), end - 0.5 * np.pi),
        polar2xy(inner, start) + polar2xy(opt * (1 - width), start + 0.5 * np.pi),
        polar2xy(inner, start),
        polar2xy(radius, start),
    ]

    codes = [Path.MOVETO,
             Path.CURVE4,
             Path.CURVE4,
             Path.CURVE4,
             Path.LINETO,
             Path.CURVE4,
             Path.CURVE4,
             Path.CURVE4,
             Path.CLOSEPOLY,
             ]

    if ax is None:
        return verts, codes
    else:
        path = Path(verts, codes)
        patch = patches.PathPatch(path, facecolor=color + (0.5,), edgecolor=color + (0.4,), lw=LW)
        ax.add_patch(patch)


def chord_arc(start1=0, end1=60, start2=180, end2=240, radius=1.0, chordwidth=0.7, ax=None, color=(1, 0, 0)):
    # start, end should be in [0, 360)
    if start1 > end1:
        start1, end1 = end1, start1
    if start2 > end2:
        start2, end2 = end2, start2
    start1 *= np.pi / 180.
    end1 *= np.pi / 180.
    start2 *= np.pi / 180.
    end2 *= np.pi / 180.
    opt1 = 4. / 3. * np.tan((end1 - start1) / 4.) * radius
    opt2 = 4. / 3. * np.tan((end2 - start2) / 4.) * radius
    rchord = radius * (1 - chordwidth)
    verts = [
        polar2xy(radius, start1),
        polar2xy(radius, start1) + polar2xy(opt1, start1 + 0.5 * np.pi),
        polar2xy(radius, end1) + polar2xy(opt1, end1 - 0.5 * np.pi),
        polar2xy(radius, end1),
        polar2xy(rchord, end1),
        polar2xy(rchord, start2),
        polar2xy(radius, start2),
        polar2xy(radius, start2) + polar2xy(opt2, start2 + 0.5 * np.pi),
        polar2xy(radius, end2) + polar2xy(opt2, end2 - 0.5 * np.pi),
        polar2xy(radius, end2),
        polar2xy(rchord, end2),
        polar2xy(rchord, start1),
        polar2xy(radius, start1),
    ]

    codes = [Path.MOVETO,
             Path.CURVE4,
             Path.CURVE4,
             Path.CURVE4,
             Path.CURVE4,
             Path.CURVE4,
             Path.CURVE4,
             Path.CURVE4,
             Path.CURVE4,
             Path.CURVE4,
             Path.CURVE4,
             Path.CURVE4,
             Path.CURVE4,
             ]

    if ax is None:
        return verts, codes
    else:
        path = Path(verts, codes)
        patch = patches.PathPatch(path, facecolor=color + (0.5,), edgecolor=color + (0.4,), lw=LW)
        ax.add_patch(patch)


def self_chord_arc(start=0, end=60, radius=1.0, chordwidth=0.7, ax=None, color=(1, 0, 0)):
    # start, end should be in [0, 360)
    if start > end:
        start, end = end, start
    start *= np.pi / 180.
    end *= np.pi / 180.
    opt = 4. / 3. * np.tan((end - start) / 4.) * radius
    rchord = radius * (1 - chordwidth)
    verts = [
        polar2xy(radius, start),
        polar2xy(radius, start) + polar2xy(opt, start + 0.5 * np.pi),
        polar2xy(radius, end) + polar2xy(opt, end - 0.5 * np.pi),
        polar2xy(radius, end),
        polar2xy(rchord, end),
        polar2xy(rchord, start),
        polar2xy(radius, start),
    ]

    codes = [Path.MOVETO,
             Path.CURVE4,
             Path.CURVE4,
             Path.CURVE4,
             Path.CURVE4,
             Path.CURVE4,
             Path.CURVE4,
             ]

    if ax is None:
        return verts, codes
    else:
        path = Path(verts, codes)
        patch = patches.PathPatch(path, facecolor=color + (0.5,), edgecolor=color + (0.4,), lw=LW)
        ax.add_patch(patch)


def chord_diagram(x, ax, colors=None, width=0.1, pad=2, chordwidth=0.7):
    """Plot a chord diagram
    Parameters
    ----------
    x :
        flux data, X[i, j] is the flux from i to j
    ax :
        matplotlib `axes` to show the plot
    colors : optional
        user defined colors in rgb format. Use function hex2rgb() to convert hex color to rgb color.
        Default: d3.js category10
    width : optional
        width/thickness of the ideogram arc
    pad : optional
        gap pad between two neighboring ideogram arcs, unit: degree, default: 2 degree
    chordwidth : optional
        position of the control points for the chords, controlling the shape of the chords
    """
    # X[i, j]:  i -> j
    x = x.sum(axis=1)  # sum over rows
    ax.set_xlim(-1.1, 1.1)
    ax.set_ylim(-1.1, 1.1)

    if colors is None:
        # use d3.js category10 https://github.com/d3/d3-3.x-api-reference/blob/master/Ordinal-Scales.md#category10
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
                  '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
        if len(x) > 10:
            print('x is too large! Use x smaller than 10')
        colors = [hex2rgb(colors[i]) for i in range(len(x))]

    # find position for each start and end
    y = x / np.sum(x).astype(float) * (360 - pad * len(x))

    pos = {}
    arc = []
    nodepos = []
    start = 0
    for i in range(len(x)):
        end = start + y[i]
        arc.append((start, end))
        angle = 0.5 * (start + end)
        # print(start, end, angle)
        if -30 <= angle <= 210:
            angle -= 90
        else:
            angle -= 270
        nodepos.append(tuple(polar2xy(1.1, 0.5 * (start + end) * np.pi / 180.)) + (angle,))
        z = (x[i, :] / x[i].astype(float)) * (end - start)
        ids = np.argsort(z)
        z0 = start
        for j in ids:
            pos[(i, j)] = (z0, z0 + z[j])
            z0 += z[j]
        start = end + pad

    for i in range(len(x)):
        start, end = arc[i]
        ideogram_arc(start=start, end=end, radius=1.0, ax=ax, color=colors[i], width=width)
        start, end = pos[(i, i)]
        self_chord_arc(start, end, radius=1. - width, color=colors[i], chordwidth=chordwidth * 0.7, ax=ax)
        for j in range(i):
            color = colors[i]
            if x[i, j] > x[j, i]:
                color = colors[j]
            start1, end1 = pos[(i, j)]
            start2, end2 = pos[(j, i)]
            chord_arc(start1, end1, start2, end2,
                      radius=1. - width, color=colors[i], chordwidth=chordwidth, ax=ax)

    return nodepos
