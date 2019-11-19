#include "bzip2.h"

#include <iostream>

int main( int argc, char** argv )
{
    BZ2Reader reader( argv[1] );
    auto i = reader.writeData( STDOUT_FILENO );
    //std::cerr << "stored CRC:" << std::hex << reader.storedCRC() << std::dec << "\n";
    //std::cerr << "total CRC:"  << std::hex << reader.totalCRC() << std::dec << "\n";
    //std::cerr << "Return code: " << i << "\n";
    if ( ( i == BZ2Reader::RETVAL_LAST_BLOCK ) && ( reader.streamCRC() == reader.totalCRC() ) ) {
        i = 0;
    }
    return 0;
}
