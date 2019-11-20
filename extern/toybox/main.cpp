#include "bzip2.h"

#include <iostream>


template<typename T1, typename T2>
std::ostream&
operator<<( std::ostream& out, std::map<T1,T2> data )
{
    for ( auto it = data.begin(); it != data.end(); ++it ) {
        out << "  " << it->first << " : " << it->second << "\n";
    }
    return out;
}


int main( int argc, char** argv )
{
    BZ2Reader reader( argv[1] );
    const auto nBytesWritten = reader.read( STDOUT_FILENO );
    //const auto nBytesWritten = reader.read( STDOUT_FILENO, nullptr, 2447359 );
    std::cerr << "stored CRC     : 0x" << std::hex << reader.streamCRC() << std::dec << "\n";
    std::cerr << "calculated CRC : 0x" << std::hex << reader.totalCRC() << std::dec << "\n";
    if ( reader.finished() && ( reader.streamCRC() != reader.totalCRC() ) ) {
        std::cerr << "Stream CRC invalid!\n";
    }
    std::cerr << "Blocksize      : " << reader.blockSize100k() * 100 << "k\n";
    std::cerr << "Stream size    : " << nBytesWritten << " B\n";
    std::cerr << "Block offsets  :\n" << reader.blockOffsets() << "\n";

    return 0;
}
