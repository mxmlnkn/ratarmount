#!/usr/bin/env python3

import numpy as np
import matplotlib.pyplot as plt
import sys, os, fnmatch, re
from itertools import cycle

def loadMemoryTracingData( fileName ):
    labels = None
    pageSize = 4096
    with open( fileName ) as file:
        for line in file:
            line = line.strip()
            if line[0] == '#':
                if labels is None:
                    labels = line[1:].strip().split( ' ' )
                elif line[1:].strip().split( '=' )[0] == 'pageSize':
                    pageSize = int( line[1:].strip().split( '=' )[1] )

    assert( 'seconds' in labels )
    iSeconds = labels.index( 'seconds' )
    assert( 'resident' in labels )
    iResident = labels.index( 'resident' )

    data = np.genfromtxt( fileName, skip_footer = 1 ).transpose()

    return data[iSeconds] - data[iSeconds][0], data[iResident] * pageSize

def plotMemoryOverTime( fileNames, plotFileName = 'memory-over-time', fileNameSizeMiB = 64 ):
    fig = plt.figure()
    ax = fig.add_subplot( 111,
        title = "Approximately {} MiB File Name Metadata".format( fileNameSizeMiB ),
        xlabel = 'Time / s',
        ylabel = 'Resident Memory / MiB'
    )

    getColorGroup = lambda x: x.split( os.sep )[-1].split( '-' )[0].split( '.' )[0]
    colorCycler = cycle( plt.rcParams['axes.prop_cycle'].by_key()['color'] )
    groupToColor = {}
    for key in sorted( list( set( [ getColorGroup( name ) for name in fileNames ] ) ) ):
        if key not in groupToColor:
            groupToColor[key] = next( colorCycler )

    def getLineStyleGroup( x ):
        fileExts = x.split( os.sep )[-1].split( '-' )[0].split( '.' )
        if len( fileExts ) > 1:
            return fileExts[-1]
        return 'uncompressed'

    groupToLineStyle = {}
    lineCycler = cycle( [ ":", "--", "-", "-." ] )
    for key in sorted( list( set( [ getLineStyleGroup( name ) for name in fileNames ] ) ) ):
        if key not in groupToLineStyle:
            groupToLineStyle[key] = next( lineCycler )

    maxTime = 0
    maxMemory = 0
    for i in range( len( fileNames ) ):
        timeSinceStart, memoryInBytes = loadMemoryTracingData( fileNames[i] )
        ax.plot( timeSinceStart, memoryInBytes / 1024.**2,
                 #label = fileNames[i].split( '/' )[-1].split( '-' )[0],
                 linestyle = groupToLineStyle[getLineStyleGroup( fileNames[i] )],
                 color = groupToColor[getColorGroup( fileNames[i] )] )

        maxTime = np.max( [ maxTime, np.max( timeSinceStart ) ] )
        maxMemory = np.max( [ maxMemory, np.max( memoryInBytes ) / 1024.**2 ] )

    for backend, color in groupToColor.items():
        ax.plot( [], [], label = backend, color = color )

    for compression, linestyle in groupToLineStyle.items():
        ax.plot( [], [], label = compression, color = '0.5', linestyle = linestyle )

    ax.set_xlim( [ 0, maxTime ] )
    ax.set_ylim( [ 0, maxMemory ] )

    ax.legend( loc = 'best' )
    fig.tight_layout()
    fig.savefig( plotFileName + '-{}-MiB-metadata.png'.format( fileNameSizeMiB ) )

def plotPerformanceComparison( fileName, fileNameSizeMiB = 64 ):
    widthBetweenBars = 1
    pageSize = 4096

    # load column labels from file
    labels = None
    groupLabels = []
    with open( fileName ) as file:
        for line in file:
            line = line.strip()
            if line[0] == '#':
                labels = line[1:].strip().split( ' ' )
            elif '#' in line:
                backend = line.split( '#' )[-1].strip()
                if backend in groupLabels:
                    break
                else:
                    groupLabels.append( backend )

    compressionGroups = []
    compressions = []
    for groupLabel in groupLabels:
        compression = groupLabel.split( '.' )[-1] if len( groupLabel.split( '.' ) ) >= 2 else ''
        compressions.append( compression )
        if compression not in compressionGroups:
            compressionGroups.append( compression )

    assert( len( groupLabels ) % len( compressionGroups ) == 0 )
    for i in range( len( compressions ) // len( compressionGroups ) ):
        assert( compressions[ len( compressionGroups ) * i : len( compressionGroups ) * ( i + 1 )] == compressionGroups )

    backends = [ x.split( '.' )[0] for x in groupLabels ]
    from collections import OrderedDict
    backends = list( OrderedDict.fromkeys( backends ) )

    rawData = np.genfromtxt( fileName ).transpose()
    rawData = rawData[:,rawData[0] == fileNameSizeMiB]

    data = {}
    for iLabel in range( len( labels ) ):
        data[labels[iLabel]] = rawData[iLabel]

    fig = plt.figure( figsize = ( 12, 6 ) )

    columnLabels = [ 'indexCreationTime', 'serializationTime', 'deserializationTime',
                     'serializedSize', 'peakRssSizeCreation', 'peakRssSizeLoading' ]
                     #'peakVmSizeCreation', 'peakVmSizeLoading' ]
    for i in range( len( columnLabels ) ):
        title = ' '.join( [ x.capitalize().replace( 'Rss', 'Resident' )
                            for x in re.sub( r'([A-Z])', r' \1', columnLabels[i] ).split( ' ' ) ] )
        ax = fig.add_subplot( 2, 3, 1+i,
            title = title,
            ylabel = "Time / s" if 'Time' in columnLabels[i] else "Size / MiB",
        )

        yData = data[columnLabels[i]]
        if 'Size' in columnLabels[i]:
            yData /= 1024.**2
            if columnLabels[i] != 'serializedSize':
                yData *= pageSize

        iBest = np.argmin( yData )
        print( "Best for", title, ":", groupLabels[iBest], 'with', yData[iBest] )

        for i in np.arange( len( yData ) // len( compressionGroups ) ):
            bar, = \
            ax.bar( len( compressionGroups ) * ( i + widthBetweenBars ) + 0,
                    yData[len( compressionGroups ) * i + 0], hatch = ''   )
            ax.bar( len( compressionGroups ) * ( i + widthBetweenBars ) + 1,
                    yData[len( compressionGroups ) * i + 1], hatch = '//', color = bar.get_facecolor() )
            ax.bar( len( compressionGroups ) * ( i + widthBetweenBars ) + 2,
                    yData[len( compressionGroups ) * i + 2], hatch = 'o' , color = bar.get_facecolor() )
            ax.bar( [ len( compressionGroups ) * widthBetweenBars ], [ 0 ],
                    color = bar.get_facecolor(), label = backends[i] )

    print()

    ax.bar( [ len( compressionGroups ) * widthBetweenBars ], [ 0 ], hatch = ''  , color = '0.9',
            label = compressionGroups[0] if compressionGroups[0] else 'uncompressed' )
    ax.bar( [ len( compressionGroups ) * widthBetweenBars ], [ 0 ], hatch = '//', color = '0.9',
            label = compressionGroups[1] if compressionGroups[1] else 'uncompressed' )
    ax.bar( [ len( compressionGroups ) * widthBetweenBars ], [ 0 ], hatch = 'o' , color = '0.9',
            label = compressionGroups[2] if compressionGroups[2] else 'uncompressed' )

    ax.legend( loc = 'center left',  bbox_to_anchor = ( 1, 0.5 ) )
    fig.tight_layout()
    fig.savefig( 'performance-comparison-{}-MiB-metadata.png'.format( fileNameSizeMiB ) )

if len( sys.argv ) != 2 or not os.path.isdir( sys.argv[1] ):
    print( "First argument must be path to data directory" )
    sys.exit( 1 )

dataFolder = sys.argv[1]
os.chdir( dataFolder )

for sizeMiB in [ 64, 256 ]:
    for type in [ 'saving', 'loading' ]:
        files = [ f for f in os.listdir( '.' ) if fnmatch.fnmatch( f, '*-' + str( sizeMiB ) + '-MiB-' + type + '.dat' ) ]
        plotMemoryOverTime( files, 'resident-memory-over-time-' + type, sizeMiB )
    plotPerformanceComparison( '../../serializationBenchmark.dat', sizeMiB )

plt.show()
