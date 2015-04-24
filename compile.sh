if [ ! -d "build" ]; then
    mkdir build
fi
if [ ! -d "deploy" ]; then
    mkdir deploy
fi
cd build
cmake ..
make 
