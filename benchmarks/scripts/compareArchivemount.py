#!/usr/bin/env python3

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import sys, os, fnmatch, re
from itertools import cycle


lineStyles = [ '-', '--', ':', '-.' ]
colors = [ '#1f77b4', '#d62728', 'g' ] # https://matplotlib.org/3.1.1/_images/dflt_style_changes-1.png
markers = [ '*', '+', 'o', 'x' ]

def axisValueReduction( ax, axis, reduction, init ):
    import numpy as np

    result = init
    # this is bugged when using axvline or axhline, because it doesn't ignore
    # the huge values set by those functions. Workaround: Call autoRange
    # before ax[v|h]line, but that is not always wanted
    for line in ax.get_lines():
        # have to use numpy here, because of:
        # https://stackoverflow.com/questions/4237914/python-max-min-builtin-functions-depend-on-parameter-order
        x = np.array( line.get_xdata(), dtype = np.float )
        y = np.array( line.get_ydata(), dtype = np.float )

        # mask not only NaNs per each x,y, but also mask all y-values whose
        # corresponding x-values are NaN!
        unmasked = np.logical_and( np.logical_not( np.isnan( x ) ), np.logical_not( np.isnan( y ) ) )
        x = x[unmasked]
        y = y[unmasked]
        unmasked = np.array( [ True ] * len( x ), dtype = np.bool )
        if axis == 'x' and ax.get_xscale() == 'log':
            np.logical_and( unmasked, x > 0 )
        if axis == 'y' and ax.get_yscale() == 'log':
            np.logical_and( unmasked, y > 0 )

        result = reduction( np.concatenate( [ [ result ], ( x if axis == 'x' else y )[unmasked] ] ) )
    return result


def loadData( fileName ):
    labels = None
    data = {}

    with open( fileName ) as file:
        for line in file:
            line = line.strip()
            if line[0] == '#':
                if labels is None:
                    labels = line[1:].strip().split( ' ' )
            else:
                row = line.split( ';' )

                tool = row[0].strip( '"' )
                if tool not in data:
                    data[tool] = {}

                # Not interested in command arguments like cat <file>
                command = row[1].strip( '"' ).split( ' ' )[0]
                if command == tool:
                    command = "mount"
                if command not in data[tool]:
                    data[tool][command] = {}

                compression = row[2].strip( '"' )
                if compression not in data[tool][command]:
                    data[tool][command][compression] = []

                data[tool][command][compression] += [ np.array( row[3:], dtype = 'float' ) ]

    for key, value in data.items():
        for command, values in value.items():
            for compression, values2 in values.items():
                data[key][command][compression] = np.array( values2, dtype = 'float' ).transpose()

    return labels[3:], data

def plotBenchmark( fileName, ax, command, metric ):
    labels, data = loadData( fileName )
    tools = [ 'archivemount', 'ratarmount' ]
    compressions = None
    fileSizes = None
    xs = np.array( [] )
    ys = np.array( [] )

    for i, tool in enumerate( tools ):
        if compressions is None:
            compressions = sorted( data[tool][command].keys() )
        else:
            assert compressions == sorted( data[tool][command].keys() )

        for j, compression in enumerate( compressions ):
            values = data[tool][command][compression]

            newFileSizes = np.sort( np.unique( values[labels.index( "nBytesPerFile" )] ) )
            if fileSizes is None:
                fileSizes = newFileSizes
            else:
                assert np.all( fileSizes == newFileSizes )

            files = values[labels.index( "nFolders" )] * \
                    values[labels.index( "nFilesPerFolder" )]
            metricValues = values[labels.index( metric )]
            bytesPerFile = values[labels.index( "nBytesPerFile" )]

            for k, nBytesPerFile in enumerate( fileSizes ):
                iSorted = np.argsort( files )
                iSelected = bytesPerFile[iSorted] == nBytesPerFile

                x = files[iSorted][iSelected]
                y = metricValues[iSorted][iSelected]

                xu = np.sort( np.unique( x ) )
                if len( xu ) == len( x ):
                    ax.plot( x, y, linestyle = lineStyles[j], color = colors[i], marker = markers[k] )
                else:
                    ymean = np.array( [ np.mean( y[x == xi] ) for xi in xu ] )
                    ymin  = np.array( [ np.min ( y[x == xi] ) for xi in xu ] )
                    ymax  = np.array( [ np.max ( y[x == xi] ) for xi in xu ] )
                    lines = ax.errorbar( xu, ymean, [ ymean - ymin, ymax - ymean ],
                                         linestyle = lineStyles[j], color = colors[i],
                                         marker = markers[k], capsize = 3 )
                    lines[-1][0].set_linestyle( lineStyles[j] )

                xs = np.append( xs, x )
                ys = np.append( ys, y )

    x = 10 ** np.linspace( np.log10( np.min( xs ) ), np.log10( np.max( xs ) ) )
    y = x;
    y = 2 * y / y[-1] * np.max( ys )
    ax.plot( x[y > np.min(ys)], y[y > np.min(ys)], color = 'k', label = "linear scaling" )

    for i, tool in enumerate( tools ):
        ax.plot( [ None ], [ None ], color = colors[i], label = tool )
    for j, compression in enumerate( compressions ):
        ax.plot( [ None ], [ None ], linestyle = lineStyles[j], color = '0.5', label = compression )
    for k, nBytesPerFile in enumerate( fileSizes ):
        ax.plot( [ None ], [ None ], linestyle = '', marker = markers[k], color = '0.5', label = "{}B per File".format( int( nBytesPerFile ) ) )

def plotPeakMemory( fileName ):
    fig = plt.figure( figsize = ( 10, 8 ) )
    ax = fig.add_subplot( 221,
        title = "Peak Resident Memory Usage During Mounting",
        xlabel = "Number of Files in Archive",
        ylabel = "Memory Usage / kiB",
        xscale = 'log',
        yscale = 'log',
    )

    plotBenchmark( fileName, ax, "mount", "peakRssMemory/kiB" )

    ax.legend( loc = 'best' )

    ax = fig.add_subplot( 222,
        title = "Time Required for Mounting",
        xlabel = "Number of Files in Archive",
        ylabel = "Runtime / s",
        xscale = 'log',
        yscale = 'log',
    )

    plotBenchmark( fileName, ax, "mount", "duration/s" )

    xmin = axisValueReduction( ax, 'x', np.nanmin, float( '+inf' ) )
    xmax = axisValueReduction( ax, 'x', np.nanmax, float( '-inf' ) )
    ymin = axisValueReduction( ax, 'y', np.nanmin, float( '+inf' ) )
    ymax = axisValueReduction( ax, 'y', np.nanmax, float( '-inf' ) )
    x = 10 ** np.linspace( np.log10( xmin ), np.log10( xmax ) )
    y = x**2;
    y = y / y[-1] * ymax / 100
    ax.plot( x[y > ymin], y[y > ymin], color = 'k', linestyle = '--', label = "quadratig scaling" )

    ax.legend( [ Line2D( [], [], linestyle = '--', color = 'k' ) ], [ 'quadratic scaling' ],  loc = 'best' )

    ax = fig.add_subplot( 223,
        title = "Time Required to Get Contents of One File",
        xlabel = "Number of Files in Archive",
        ylabel = "Runtime / s",
        xscale = 'log',
        yscale = 'log',
    )

    plotBenchmark( fileName, ax, "cat", "duration/s" )

    #ax = fig.add_subplot( 224,
    #    title = "Time Required for Getting Metadata of One File",
    #    xlabel = "Number of Files in Archive",
    #    ylabel = "Runtime / s",
    #    xscale = 'log',
    #    yscale = 'log',
    #)
    #
    #plotBenchmark( fileName, ax, "stat", "duration/s" )

    ax = fig.add_subplot( 224,
        title = "Time Required for Listing All Files",
        xlabel = "Number of Files in Archive",
        ylabel = "Runtime / s",
        xscale = 'log',
        yscale = 'log',
    )

    plotBenchmark( fileName, ax, "find", "duration/s" )

    fig.tight_layout()
    fig.savefig( 'archivemount-comparison.png', dpi = 150 )


if __name__ == "__main__":
    if len( sys.argv ) != 2 or not os.path.isfile( sys.argv[1] ):
        print( "First argument must be path to data file" )
        sys.exit( 1 )

    dataFile = sys.argv[1]

    plotPeakMemory( dataFile )
