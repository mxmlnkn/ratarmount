/* bzcat.c - decompress stdin to stdout using bunzip2.
 *
 * Copyright 2003, 2007 Rob Landley <rob@landley.net>
 *
 * Based on a close reading (but not the actual code) of the original bzip2
 * decompression code by Julian R Seward (jseward@acm.org), which also
 * acknowledges contributions by Mike Burrows, David Wheeler, Peter Fenwick,
 * Alistair Moffat, Radford Neal, Ian H. Witten, Robert Sedgewick, and
 * Jon L. Bentley.
 *
 * No standard.
 */

#include <stdint.h>
#include <limits.h>
#include <stdio.h>
#include <unistd.h>

#include <cassert>
#include <cstring>
#include <array>
#include <sstream>
#include <stdexcept>
#include <vector>


class BZ2Reader
{
public:
    /* Constants for huffman coding */
    static constexpr int MAX_GROUPS = 6;
    static constexpr int GROUP_SIZE = 50;       /* 64 would have been more efficient */
    static constexpr int MAX_HUFCODE_BITS = 20; /* Longest huffman code allowed */
    static constexpr int MAX_SYMBOLS = 258;     /* 256 literals + RUNA + RUNB */
    static constexpr int SYMBOL_RUNA = 0;
    static constexpr int SYMBOL_RUNB = 1;

    static constexpr int IOBUF_SIZE = 4096;
    static constexpr int RETVAL_LAST_BLOCK = -100;
    static constexpr int THREADS = 1;

private:
    // This is what we know about each huffman coding group
    struct group_data
    {
        int limit[MAX_HUFCODE_BITS + 1], base[MAX_HUFCODE_BITS], permute[MAX_SYMBOLS];
        char minLen, maxLen;
    };

    // Data for burrows wheeler transform

    struct bwdata
    {
        unsigned int origPtr;
        int byteCount[256];
        // State saved when interrupting output
        int writePos, writeRun, writeCount, writeCurrent;
        unsigned int dataCRC, headerCRC;
        unsigned int* dbuf;
    };

    // Structure holding all the housekeeping data, including IO buffers and
    // memory that persists between calls to bunzip
    struct bunzip_data
    {
        // Input stream, input buffer, input bit buffer
        int in_fd, inbufCount, inbufPos;
        unsigned char* inbuf;
        unsigned int inbufBitCount, inbufBits;

        // Output buffer
        char outbuf[IOBUF_SIZE];
        int outbufPos;

        unsigned int totalCRC;

        // First pass decompression data (Huffman and MTF decoding)
        char selectors[32768];                // nSelectors=15 bits
        struct group_data groups[MAX_GROUPS]; // huffman coding tables
        int symTotal, groupCount, nSelectors;
        unsigned char symToByte[256], mtfSymbol[256];

        // The CRC values stored in the block header and calculated from the data
        unsigned int crc32Table[256];

        // Second pass decompression data (burrows-wheeler transform)
        unsigned int dbufSize;
        struct bwdata bwdata[THREADS];
    };

    struct BlockHeader
    {
        uint64_t compressed_magic;
        bool randomised;
        /**
         * The Mapping table itself is compressed in two parts:
         * huffman_used_map: each bit indicates whether the corresponding range [0...15], [16...31] is present.
         * huffman_used_bitmaps: 0-16 16-bit bitmaps
         * The Huffman map gives 0, 10, 11, 100, 101, ... (8-bit) symbols
         * Instead of storing 2 * 256 bytes ( 0b : A, 10b : B, .. ) for the table, the first part is left out.
         * And for short maps, only the first n-th are actually stored.
         * The second half is also assumed to be ordered, so that we only need to store which symbols are actually
         * present.
         * This however means that the Huffmann table can't be frequency sorted, therefore this is done in a
         * second step / table, the mtfSymbol (move to front) map.
         * This would need 256 bits to store the table in huffman_used_bitmaps.
         * These bits are split in groups of 16 and the presence of each group is encoded in huffman_used_map
         * to save even more bytes.
         * @verbatim
         *  10001000 00000000     # huffman_used_map (bit map)
         *  ^   ^
         *  |   [64,95]
         *  [0...15]
         *  00000000 00100000     # huffman_used_bitmaps[0]
         *  ^          ^    ^
         *  0          10   15
         *          (newline)
         *  00000100 10001001     # huffman_used_bitmaps[1]
         *  ^    ^   ^   ^  ^
         *  64   69  72  76 95
         *       E   H   L  O
         * @endverbatim
         */
        uint16_t huffman_used_map;
        /**
         * mapping table: if some byte values are never used (encoding things
         * like ascii text), the compression code removes the gaps to have fewer
         * symbols to deal with, and writes a sparse bitfield indicating which
         * values were present.  We make a translation table to convert the symbols
         * back to the corresponding bytes.
         */
        std::array<unsigned char, 256> symbolToByte;
        unsigned char symbolCount;
        unsigned char huffman_groups;
        /**
         * Every GROUP_SIZE many symbols we switch huffman coding tables.
         * Each group has a selector, which is an index into the huffman coding table arrays.
         *
         * Read in the group selector array, which is stored as MTF encoded
         * bit runs.  (MTF = Move To Front.  Every time a symbol occurs it's moved
         * to the front of the table, so it has a shorter encoding next time.)
         */
        uint16_t selectors_used;
    };

public:
    BZ2Reader( const std::string& filePath )
    {
        m_file = fopen( filePath.c_str(), "r" );
        startBunzip( fileno( m_file ), nullptr, 0 );
    }

    BZ2Reader( const int fileDescriptor )
    {
        startBunzip( fileDescriptor, nullptr, 0 );
    }

    BZ2Reader( const char*  bz2Data,
               const size_t size )
    {
        startBunzip( 0, bz2Data, size );
    }

    ~BZ2Reader()
    {
        if ( m_file != nullptr ) {
            fclose( m_file );
        }
        delete[] bd.inbuf;
        for ( int i = 0; i < THREADS; i++ ) {
            delete[] bd.bwdata[i].dbuf;
        }
    }

    uint32_t
    streamCRC() const
    {
        return bd.bwdata[0].headerCRC; // only true when called after last block read
    }

    uint32_t
    totalCRC() const
    {
        return bd.totalCRC;
    }

    // Decompress a block of text to intermediate buffer
    int
    readNextBlock();

    int
    decodeBuffer( struct bwdata* bw,
                  int            out_fd,
                  char*          outbuf,
                  int            len );

    int
    writeData( const int   outputFileDescriptor,
               char* const outputBuffer = nullptr,
               const int   outputBufferSize = 0 )
    {
        const auto rc = decodeBuffer( &bd.bwdata[0], outputFileDescriptor, outputBuffer, outputBufferSize );
        flushOutputBuffer( outputFileDescriptor );
        if ( ( rc == RETVAL_LAST_BLOCK ) && ( bd.bwdata[0].headerCRC == bd.totalCRC ) ) {
            return 0;
        }
        return rc;
    }

private:
    void
    flushOutputBuffer( int outputFileDescriptor );

    void
    prepareBurrowsWheeler( struct bwdata* bw );

    int
    read_block_header( struct bwdata* bw );

    int
    read_huffman_data( struct bwdata* bw );

    unsigned int
    get_bits( char bits_wanted );

    uint32_t
    getBits( const unsigned char bits_wanted );

    static std::vector<uint32_t>
    createCRC32LookupTable( size_t entryCount = 256,
                            bool   littleEndian = false );

    static void
    burrows_wheeler_prep( struct bwdata* bw );

    void
    startBunzip( const int    fileDescriptor,
                 const char*  bz2Data,
                 const size_t size );

private:
    bunzip_data bd;
    FILE* m_file;
};


// Return the next nnn bits of input.  All reads from the compressed input
// are done through this function.  All reads are big endian.
inline unsigned int
BZ2Reader::get_bits( char bits_wanted )
{
    unsigned int bits = 0;

    // If we need to get more data from the byte buffer, do so.  (Loop getting
    // one byte at a time to enforce endianness and avoid unaligned access.)
    while ( bd.inbufBitCount < bits_wanted ) {
        // If we need to read more data from file into byte buffer, do so
        if ( bd.inbufPos == bd.inbufCount ) {
            if ( 0 >= ( bd.inbufCount = read( bd.in_fd, bd.inbuf, IOBUF_SIZE ) ) ) {
                throw std::domain_error( "Not enough data to read!" );
            }
            bd.inbufPos = 0;
        }

        // Avoid 32-bit overflow (dump bit buffer to top of output)
        if ( bd.inbufBitCount >= 24 ) {
            bits = bd.inbufBits & ( ( 1 << bd.inbufBitCount ) - 1 );
            bits_wanted -= bd.inbufBitCount;
            bits <<= bits_wanted;
            bd.inbufBitCount = 0;
        }

        // Grab next 8 bits of input from buffer.
        bd.inbufBits = ( bd.inbufBits << 8 ) | bd.inbuf[bd.inbufPos++];
        bd.inbufBitCount += 8;
    }

    // Calculate result
    bd.inbufBitCount -= bits_wanted;
    bits |= ( bd.inbufBits >> bd.inbufBitCount ) & ( ( 1 << bits_wanted ) - 1 );

    return bits;
}


/* Read block header at start of a new compressed data block.  Consists of:
 *
 * 48 bits : Block signature, either pi (data block) or e (EOF block).
 * 32 bits : bw->headerCRC
 * 1  bit  : obsolete feature flag.
 * 24 bits : origPtr (Burrows-wheeler unwind index, only 20 bits ever used)
 * 16 bits : Mapping table index.
 *[16 bits]: symToByte[symTotal] (Mapping table.  For each bit set in mapping
 *           table index above, read another 16 bits of mapping table data.
 *           If correspondig bit is unset, all bits in that mapping table
 *           section are 0.)
 *  3 bits : groupCount (how many huffman tables used to encode, anywhere
 *           from 2 to MAX_GROUPS)
 * variable: hufGroup[groupCount] (MTF encoded huffman table data.)
 */
inline int
BZ2Reader::read_block_header( struct bwdata* bw )
{
    struct group_data* hufGroup;
    int hh, ii, jj, kk, symCount, * base, * limit;
    unsigned char uc;

    // Read in header signature and CRC (which is stored big endian)
    ii = get_bits( 24 );
    jj = get_bits( 24 );
    bw->headerCRC = get_bits( 32 );

    // Is this the EOF block with CRC for whole file?  (Constant is "e")
    if ( ( ii == 0x177245 ) && ( jj == 0x385090 ) ) {
        return RETVAL_LAST_BLOCK;
    }

    // Is this a valid data block?  (Constant is "pi".)
    if ( ( ii != 0x314159 ) || ( jj != 0x265359 ) ) {
        std::stringstream msg;
        msg << "[BZip2 block header] invalid compressed magic 0x" << std::hex << ii << jj;
        throw std::domain_error( msg.str() );
    }

    if ( get_bits( 1 ) ) {
        throw std::domain_error( "[BZip2 block header] deprecated randomised bit is not supported" );
    }

    if ( ( bw->origPtr = get_bits( 24 ) ) > bd.dbufSize ) {
        std::stringstream msg;
        msg << "[BZip2 block header] origPtr " << bw->origPtr << " is larger than buffer size: " << bd.dbufSize;
        throw std::logic_error( msg.str() );
    }

    // mapping table: if some byte values are never used (encoding things
    // like ascii text), the compression code removes the gaps to have fewer
    // symbols to deal with, and writes a sparse bitfield indicating which
    // values were present.  We make a translation table to convert the symbols
    // back to the corresponding bytes.
    hh = get_bits( 16 );
    bd.symTotal = 0;
    for ( ii = 0; ii < 16; ii++ ) {
        if ( hh & ( 1 << ( 15 - ii ) ) ) {
            kk = get_bits( 16 );
            for ( jj = 0; jj < 16; jj++ ) {
                if ( kk & ( 1 << ( 15 - jj ) ) ) {
                    bd.symToByte[bd.symTotal++] = ( 16 * ii ) + jj;
                }
            }
        }
    }

    // How many different huffman coding groups does this block use?
    bd.groupCount = get_bits( 3 );
    if ( ( bd.groupCount < 2 ) || ( bd.groupCount > MAX_GROUPS ) ) {
        std::stringstream msg;
        msg << "[BZip2 block header] Invalid Huffman coding group count " << bd.groupCount;
        throw std::logic_error( msg.str() );
    }

    // nSelectors: Every GROUP_SIZE many symbols we switch huffman coding
    // tables.  Each group has a selector, which is an index into the huffman
    // coding table arrays.
    //
    // Read in the group selector array, which is stored as MTF encoded
    // bit runs.  (MTF = Move To Front.  Every time a symbol occurs it's moved
    // to the front of the table, so it has a shorter encoding next time.)
    if ( !( bd.nSelectors = get_bits( 15 ) ) ) {
        std::stringstream msg;
        msg << "[BZip2 block header] selectors_used " << bd.nSelectors << " is invalid";
        throw std::logic_error( msg.str() );
    }
    for ( ii = 0; ii < bd.groupCount; ii++ ) {
        bd.mtfSymbol[ii] = ii;
    }
    for ( ii = 0; ii < bd.nSelectors; ii++ ) {
        // Get next value
        for ( jj = 0; get_bits( 1 ); jj++ ) {
            if ( jj >= bd.groupCount ) {
                std::stringstream msg;
                msg << "[BZip2 block header] Could not find zero termination after " << bd.groupCount << " bits";
                throw std::domain_error( msg.str() );
            }
        }

        // Decode MTF to get the next selector, and move it to the front.
        uc = bd.mtfSymbol[jj];
        memmove( bd.mtfSymbol + 1, bd.mtfSymbol, jj );
        bd.mtfSymbol[0] = bd.selectors[ii] = uc;
    }

    // Read the huffman coding tables for each group, which code for symTotal
    // literal symbols, plus two run symbols (RUNA, RUNB)
    symCount = bd.symTotal + 2;
    for ( jj = 0; jj < bd.groupCount; jj++ ) {
        unsigned char length[MAX_SYMBOLS];
        unsigned temp[MAX_HUFCODE_BITS + 1];
        int minLen, maxLen, pp;

        // Read lengths
        hh = get_bits( 5 );
        for ( ii = 0; ii < symCount; ii++ ) {
            for ( ; ; ) {
                // !hh || hh > MAX_HUFCODE_BITS in one test.
                if ( MAX_HUFCODE_BITS - 1 < (unsigned)hh - 1 ) {
                    std::stringstream msg;
                    msg << "[BZip2 block header]  start_huffman_length " << hh
                    << " is larger than " << MAX_HUFCODE_BITS << " or zero\n";
                    throw std::logic_error( msg.str() );
                }
                // Grab 2 bits instead of 1 (slightly smaller/faster).  Stop if
                // first bit is 0, otherwise second bit says whether to
                // increment or decrement.
                kk = get_bits( 2 );
                if ( kk & 2 ) {
                    hh += 1 - ( ( kk & 1 ) << 1 );
                } else {
                    bd.inbufBitCount++;
                    break;
                }
            }
            length[ii] = hh;
        }

        // Find largest and smallest lengths in this group
        minLen = maxLen = length[0];
        for ( ii = 1; ii < symCount; ii++ ) {
            if ( length[ii] > maxLen ) {
                maxLen = length[ii];
            } else if ( length[ii] < minLen ) {
                minLen = length[ii];
            }
        }

        /* Calculate permute[], base[], and limit[] tables from length[].
         *
         * permute[] is the lookup table for converting huffman coded symbols
         * into decoded symbols.  It contains symbol values sorted by length.
         *
         * base[] is the amount to subtract from the value of a huffman symbol
         * of a given length when using permute[].
         *
         * limit[] indicates the largest numerical value a symbol with a given
         * number of bits can have.  It lets us know when to stop reading.
         *
         * To use these, keep reading bits until value <= limit[bitcount] or
         * you've read over 20 bits (error).  Then the decoded symbol
         * equals permute[hufcode_value - base[hufcode_bitcount]].
         */
        hufGroup = bd.groups + jj;
        hufGroup->minLen = minLen;
        hufGroup->maxLen = maxLen;

        // Note that minLen can't be smaller than 1, so we adjust the base
        // and limit array pointers so we're not always wasting the first
        // entry.  We do this again when using them (during symbol decoding).
        base = hufGroup->base - 1;
        limit = hufGroup->limit - 1;

        // zero temp[] and limit[], and calculate permute[]
        pp = 0;
        for ( ii = minLen; ii <= maxLen; ii++ ) {
            temp[ii] = limit[ii] = 0;
            for ( hh = 0; hh < symCount; hh++ ) {
                if ( length[hh] == ii ) {
                    hufGroup->permute[pp++] = hh;
                }
            }
        }

        // Count symbols coded for at each bit length
        for ( ii = 0; ii < symCount; ii++ ) {
            temp[length[ii]]++;
        }

        /* Calculate limit[] (the largest symbol-coding value at each bit
         * length, which is (previous limit<<1)+symbols at this level), and
         * base[] (number of symbols to ignore at each bit length, which is
         * limit minus the cumulative count of symbols coded for already). */
        pp = hh = 0;
        for ( ii = minLen; ii < maxLen; ii++ ) {
            pp += temp[ii];
            limit[ii] = pp - 1;
            pp <<= 1;
            base[ii + 1] = pp - ( hh += temp[ii] );
        }
        limit[maxLen] = pp + temp[maxLen] - 1;
        limit[maxLen + 1] = INT_MAX;
        base[minLen] = 0;
    }

    return 0;
}


/* First pass, read block's symbols into dbuf[dbufCount].
 *
 * This undoes three types of compression: huffman coding, run length encoding,
 * and move to front encoding.  We have to undo all those to know when we've
 * read enough input.
 */
inline int
BZ2Reader::read_huffman_data( struct bwdata* bw )
{
    struct group_data* hufGroup;
    int hh, ii, jj, kk, runPos, dbufCount, symCount, selector, nextSym,
        * byteCount, * base, * limit;
    unsigned int* dbuf = bw->dbuf;
    unsigned char uc;

    // We've finished reading and digesting the block header.  Now read this
    // block's huffman coded symbols from the file and undo the huffman coding
    // and run length encoding, saving the result into dbuf[dbufCount++] = uc

    // Initialize symbol occurrence counters and symbol mtf table
    byteCount = bw->byteCount;
    for ( ii = 0; ii < 256; ii++ ) {
        byteCount[ii] = 0;
        bd.mtfSymbol[ii] = ii;
    }

    // Loop through compressed symbols.  This is the first "tight inner loop"
    // that needs to be micro-optimized for speed.  (This one fills out dbuf[]
    // linearly, staying in cache more, so isn't as limited by DRAM access.)
    runPos = dbufCount = symCount = selector = 0;
    // Some unnecessary initializations to shut gcc up.
    base = limit = 0;
    hufGroup = 0;
    hh = 0;

    for ( ; ; ) {
        // Have we reached the end of this huffman group?
        if ( !( symCount-- ) ) {
            // Determine which huffman coding group to use.
            symCount = GROUP_SIZE - 1;
            if ( selector >= bd.nSelectors ) {
                std::stringstream msg;
                msg << "[BZip2 block data] selector " << selector << " out of maximum range " << bd.nSelectors;
                throw std::domain_error( msg.str() );
            }
            hufGroup = bd.groups + bd.selectors[selector++];
            base = hufGroup->base - 1;
            limit = hufGroup->limit - 1;
        }

        // Read next huffman-coded symbol (into jj).
        ii = hufGroup->minLen;
        jj = get_bits( ii );
        while ( jj > limit[ii] ) {
            // if (ii > hufGroup->maxLen) return RETVAL_DATA_ERROR;
            ii++;

            // Unroll get_bits() to avoid a function call when the data's in
            // the buffer already.
            kk = bd.inbufBitCount
                 ? ( bd.inbufBits >> --( bd.inbufBitCount ) ) & 1 : get_bits( 1 );
            jj = ( jj << 1 ) | kk;
        }
        // Huffman decode jj into nextSym (with bounds checking)
        jj -= base[ii];

        if ( ii > hufGroup->maxLen ) {
            std::stringstream msg;
            msg << "[BZip2 block data] " << ii << " bigger than max length " << hufGroup->maxLen;
            throw std::domain_error( msg.str() );
        }

        if ( (unsigned)jj >= MAX_SYMBOLS ) {
            std::stringstream msg;
            msg << "[BZip2 block data] " << jj << " larger than max symbols " << MAX_SYMBOLS;
            throw std::domain_error( msg.str() );
        }

        nextSym = hufGroup->permute[jj];

        // If this is a repeated run, loop collecting data
        if ( (unsigned)nextSym <= SYMBOL_RUNB ) {
            // If this is the start of a new run, zero out counter
            if ( !runPos ) {
                runPos = 1;
                hh = 0;
            }

            /* Neat trick that saves 1 symbol: instead of or-ing 0 or 1 at
             * each bit position, add 1 or 2 instead. For example,
             * 1011 is 1<<0 + 1<<1 + 2<<2. 1010 is 2<<0 + 2<<1 + 1<<2.
             * You can make any bit pattern that way using 1 less symbol than
             * the basic or 0/1 method (except all bits 0, which would use no
             * symbols, but a run of length 0 doesn't mean anything in this
             * context). Thus space is saved. */
            hh += ( runPos << nextSym ); // +runPos if RUNA; +2*runPos if RUNB
            runPos <<= 1;
            continue;
        }

        /* When we hit the first non-run symbol after a run, we now know
         * how many times to repeat the last literal, so append that many
         * copies to our buffer of decoded symbols (dbuf) now. (The last
         * literal used is the one at the head of the mtfSymbol array.) */
        if ( runPos ) {
            runPos = 0;
            if ( dbufCount + hh > bd.dbufSize ) {
                std::stringstream msg;
                msg << "[BZip2 block data] dbufCount " << dbufCount << " > " << bd.dbufSize << " dbufSize";
                throw std::domain_error( msg.str() );
            }

            uc = bd.symToByte[bd.mtfSymbol[0]];
            byteCount[uc] += hh;
            while ( hh-- ) {
                dbuf[dbufCount++] = uc;
            }
        }

        // Is this the terminating symbol?
        if ( nextSym > bd.symTotal ) {
            break;
        }

        /* At this point, the symbol we just decoded indicates a new literal
         * character. Subtract one to get the position in the MTF array
         * at which this literal is currently to be found. (Note that the
         * result can't be -1 or 0, because 0 and 1 are RUNA and RUNB.
         * Another instance of the first symbol in the mtf array, position 0,
         * would have been handled as part of a run.) */
        if ( dbufCount >= bd.dbufSize ) {
            std::stringstream msg;
            msg << "[BZip2 block data] dbufCount " << dbufCount << " > " << bd.dbufSize << " dbufSize";
            throw std::domain_error( msg.str() );
        }
        ii = nextSym - 1;
        uc = bd.mtfSymbol[ii];
        // On my laptop, unrolling this memmove() into a loop shaves 3.5% off
        // the total running time.
        while ( ii-- ) {
            bd.mtfSymbol[ii + 1] = bd.mtfSymbol[ii];
        }
        bd.mtfSymbol[0] = uc;
        uc = bd.symToByte[uc];

        // We have our literal byte.  Save it into dbuf.
        byteCount[uc]++;
        dbuf[dbufCount++] = (unsigned int)uc;
    }

    // Now we know what dbufCount is, do a better sanity check on origPtr.
    if ( bw->origPtr >= ( bw->writeCount = dbufCount ) ) {
        std::stringstream msg;
        msg << "[BZip2 block data] origPtr error " << bw->origPtr;
        throw std::domain_error( msg.str() );
    }

    return 0;
}


inline void
BZ2Reader::flushOutputBuffer( int outputFileDescriptor )
{
    if ( bd.outbufPos ) {
        if ( write( outputFileDescriptor, bd.outbuf, bd.outbufPos ) != bd.outbufPos ) {
            throw std::logic_error( "Could not flush complete output buffer" );
        }
        bd.outbufPos = 0;
    }
}


inline void
BZ2Reader::prepareBurrowsWheeler( struct bwdata* bw )
{
    int ii, jj;
    unsigned int* dbuf = bw->dbuf;
    int* byteCount = bw->byteCount;

    // Technically this part is preparation for the burrows-wheeler
    // transform, but it's quick and convenient to do here.

    // Turn byteCount into cumulative occurrence counts of 0 to n-1.
    jj = 0;
    for ( ii = 0; ii < 256; ii++ ) {
        int kk = jj + byteCount[ii];
        byteCount[ii] = jj;
        jj = kk;
    }

    // Use occurrence counts to quickly figure out what order dbuf would be in
    // if we sorted it.
    for ( ii = 0; ii < bw->writeCount; ii++ ) {
        unsigned char uc = dbuf[ii];
        dbuf[byteCount[uc]] |= ( ii << 8 );
        byteCount[uc]++;
    }

    // blockRandomised support would go here.

    // Using ii as position, jj as previous character, hh as current character,
    // and uc as run count.
    bw->dataCRC = 0xffffffffL;

    /* Decode first byte by hand to initialize "previous" byte. Note that it
     * doesn't get output, and if the first three characters are identical
     * it doesn't qualify as a run (hence uc=255, which will either wrap
     * to 1 or get reset). */
    if ( bw->writeCount ) {
        bw->writePos = dbuf[bw->origPtr];
        bw->writeCurrent = (unsigned char)bw->writePos;
        bw->writePos >>= 8;
        bw->writeRun = -1;
    }
}


inline int
BZ2Reader::readNextBlock()
{
    int rc = read_block_header( bd.bwdata );
    if ( !rc ) {
        rc = read_huffman_data( bd.bwdata );
    }

    // First thing that can be done by a background thread.
    prepareBurrowsWheeler( bd.bwdata );

    return rc;
}


// Undo burrows-wheeler transform on intermediate buffer to produce output.
// If !len, write up to len bytes of data to buf.  Otherwise write to out_fd.
// Returns len ? bytes written : 0.  Notice all errors are negative #'s.
//
// Burrows-wheeler transform is described at:
// http://dogma.net/markn/articles/bwt/bwt.htm
// http://marknelson.us/1996/09/01/bwt/
inline int
BZ2Reader::decodeBuffer( struct bwdata* bw,
                         int            out_fd,
                         char*          outbuf,
                         int            len )
{
    unsigned int* dbuf = bw->dbuf;
    int count, pos, current, run, copies, outbyte, previous, gotcount = 0;

    for ( ; ; ) {
        // If last read was short due to end of file, return last block now
        if ( bw->writeCount < 0 ) {
            return bw->writeCount;
        }

        // If we need to refill dbuf, do it.
        if ( !bw->writeCount ) {
            int i = readNextBlock();
            if ( i ) {
                if ( i == RETVAL_LAST_BLOCK ) {
                    bw->writeCount = i;
                    return gotcount;
                } else {
                    return i;
                }
            }
        }

        // loop generating output
        count = bw->writeCount;
        pos = bw->writePos;
        current = bw->writeCurrent;
        run = bw->writeRun;
        while ( count ) {
            // If somebody (like tar) wants a certain number of bytes of
            // data from memory instead of written to a file, humor them.
            if ( len && ( bd.outbufPos >= len ) ) {
                goto dataus_interruptus;
            }
            count--;

            // Follow sequence vector to undo Burrows-Wheeler transform.
            previous = current;
            pos = dbuf[pos];
            current = pos & 0xff;
            pos >>= 8;

            // Whenever we see 3 consecutive copies of the same byte,
            // the 4th is a repeat count
            if ( run++ == 3 ) {
                copies = current;
                outbyte = previous;
                current = -1;
            } else {
                copies = 1;
                outbyte = current;
            }

            // Output bytes to buffer, flushing to file if necessary
            while ( copies-- ) {
                if ( bd.outbufPos == IOBUF_SIZE ) {
                    flushOutputBuffer( out_fd );
                }
                bd.outbuf[bd.outbufPos++] = outbyte;
                bw->dataCRC = ( bw->dataCRC << 8 )
                              ^ bd.crc32Table[( bw->dataCRC >> 24 ) ^ outbyte];
            }
            if ( current != previous ) {
                run = 0;
            }
        }

        // decompression of this block completed successfully
        bw->dataCRC = ~( bw->dataCRC );
        bd.totalCRC = ( ( bd.totalCRC << 1 ) | ( bd.totalCRC >> 31 ) ) ^ bw->dataCRC;

        // if this block had a crc error, force file level crc error.
        if ( bw->dataCRC != bw->headerCRC ) {
            bd.totalCRC = bw->headerCRC + 1;

            return RETVAL_LAST_BLOCK;
        }
dataus_interruptus:
        bw->writeCount = count;
        if ( len ) {
            gotcount += bd.outbufPos;
            memcpy( outbuf, bd.outbuf, len );

            // If we got enough data, checkpoint loop state and return
            if ( ( len -= bd.outbufPos ) < 1 ) {
                bd.outbufPos -= len;
                if ( bd.outbufPos ) {
                    memmove( bd.outbuf, bd.outbuf + len, bd.outbufPos );
                }
                bw->writePos = pos;
                bw->writeCurrent = current;
                bw->writeRun = run;

                return gotcount;
            }
        }
    }
}


inline void
BZ2Reader::startBunzip( const int    fileDescriptor,
                        const char*  bz2Data,
                        const size_t size )
{
    std::memset( &bd, 0, sizeof( bd ) );

    if ( size ) {
        bd.inbuf = new uint8_t[size];
        std::memcpy( bd.inbuf, bz2Data, size );
        bd.inbufCount = size;
        bd.in_fd = -1;
    } else {
        bd.inbuf = new uint8_t[IOBUF_SIZE];
        std::memset( bd.inbuf, 0, IOBUF_SIZE );
        bd.in_fd = fileDescriptor;
    }

    const auto crc32Table = createCRC32LookupTable();
    std::memcpy( bd.crc32Table, crc32Table.data(), sizeof( bd.crc32Table ) );

    // Ensure that file starts with "BZh".
    for ( auto i = 0; i < 3; i++ ) {
        if ( getBits( 8 ) != "BZh"[i] ) {
            throw std::domain_error( "Input header is not BZip2 magic 'BZh'" );
        }
    }

    // Next byte ascii '1'-'9', indicates block size in units of 100k of
    // uncompressed data. Allocate intermediate buffer for block.
    const auto i = getBits( 8 );
    if ( ( i < '1' ) || ( i > '9' ) ) {
        std::stringstream msg;
        msg << "[Open BZip2] Blocksize must be one of '0' (" << std::hex << (int)'0' << ") ... '9' (" << (int)'9'
        << ") but is " << i << " (" << (int)i << ")" << std::dec;
        throw std::domain_error( msg.str() );
    }
    bd.dbufSize = 100000 * ( i - '0' ) * THREADS;
    for ( int i = 0; i < THREADS; i++ ) {
        bd.bwdata[i].dbuf = new uint32_t[bd.dbufSize];
    }
}


inline std::vector<uint32_t>
BZ2Reader::createCRC32LookupTable( size_t entryCount,
                                   bool   littleEndian )
{
    // Init the CRC32 table (big endian)
    std::vector<uint32_t> table( entryCount );
    for ( size_t i = 0; i < entryCount; ++i ) {
        uint32_t c = littleEndian ? i : i << 24;
        for ( int j = 8; j != 0; --j ) {
            if ( littleEndian ) {
                c = ( c & 1 ) ? ( c >> 1 ) ^ 0xEDB88320 : c >> 1;
            } else {
                c = c & 0x80000000 ? ( c << 1 ) ^ 0x04c11db7 : ( c << 1 );
            }
        }
        table[i] = c;
    }
    return table;
}


inline uint32_t
BZ2Reader::getBits( const unsigned char bits_wanted )
{
    uint32_t bits = 0;
    assert( bits_wanted <= sizeof( bits ) * 8 );

    // If we need to get more data from the byte buffer, do so.  (Loop getting
    // one byte at a time to enforce endianness and avoid unaligned access.)
    auto bitsNeeded = bits_wanted;
    while ( bd.inbufBitCount < bitsNeeded ) {
        // If we need to read more data from file into byte buffer, do so
        if ( bd.inbufPos == bd.inbufCount ) {
            if ( 0 >= ( bd.inbufCount = read( bd.in_fd, bd.inbuf, IOBUF_SIZE ) ) ) {
                throw std::domain_error( "Not enough data to read!" );
            }
            bd.inbufPos = 0;
        }

        // Avoid 32-bit overflow (dump bit buffer to top of output)
        if ( bd.inbufBitCount >= 24 ) {
            bits = bd.inbufBits & ( ( 1 << bd.inbufBitCount ) - 1 );
            bitsNeeded -= bd.inbufBitCount;
            bits <<= bitsNeeded;
            bd.inbufBitCount = 0;
        }

        // Grab next 8 bits of input from buffer.
        bd.inbufBits = ( bd.inbufBits << 8 ) | bd.inbuf[bd.inbufPos++];
        bd.inbufBitCount += 8;
    }

    // Calculate result
    bd.inbufBitCount -= bitsNeeded;
    bits |= ( bd.inbufBits >> bd.inbufBitCount ) & ( ( 1 << bitsNeeded ) - 1 );
    assert( bits == ( bits & ( 0xffffffffL >> ( 32 - bits_wanted ) ) ) );
    return bits;
}
