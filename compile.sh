if [ ! -d "build" ]; then
    mkdir build
fi
cd build
cmake ..
make 
CTEST_OUTPUT_ON_FAILURE=TRUE make test
