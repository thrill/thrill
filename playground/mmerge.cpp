#include <iostream>
#include <mpi.h>
#include <algorithm>
#include <vector>
#include <cassert>
#include <cstring>
#include <sstream>

//Parallel Multiway-Merge using MPI

using namespace std;

typedef int T;

const int dim = 2;
const int max_data = 20;
const int data_count = 100000;

int me;
stringstream logger;

typedef std::pair<T, size_t> Pivot;

template <typename T, typename V>
void logArray(std::pair<T, V> data[], size_t len, string name) {
  stringstream ss;
  ss << me << ": " << name << ":";

  for(size_t i = 0; i < len; i++) {
    ss << " (" << data[i].first << ", " << data[i].second << "), ";
  }

  ss << endl;
  cout << ss.str();
} 

template <typename T>
void logArray(T data[], size_t len, string name) {
  stringstream ss;
  ss << me << ": " << name << ":";

  for(size_t i = 0; i < len; i++) {
    ss << " " << data[i];
  }

  ss << endl;
  cout << ss.str();
} 

vector<vector<T>> createTestEnvironment() {

  srand(me + time(NULL));

  vector<vector<T>> data(dim);

  for(int i = 0; i < dim; i++) {
    for(int j = 0; j < data_count; j++) {
      data[i].push_back((rand() % max_data)); 
    }
    sort(data[i].begin(), data[i].end());
  }

  return data;

}

vector<T> flatten(const vector<vector<T>> &data) {
  vector<T> flat; 

  for(auto part : data) {
    flat.insert(flat.end(), part.begin(), part.end());
  }

  sort(flat.begin(), flat.end());
  
  return flat;
}

//Select element with rank "rank" from data. Make sure that ||data|| < lsize 
T select(const vector<vector<T>> &data, size_t rank, size_t lsize) {

  assert(rank < lsize);

  vector<T> flat = flatten(data);

  //logger << me << ": selecting element with rank " << rank << " from " << lsize << " elements which is " << flat[rank] << endl;
  //cout << logger.str(); logger.str("");

  return flat[rank];
}

//Finds the rank of element "element" in data, or the next greatest element if data 
//does not contain "element". 
size_t find(const vector<vector<T>> &data, Pivot element, size_t localStart) {

  vector<T> flat = flatten(data);
  
  for(int i = 0; i < flat.size(); i++) {
    if(flat[i] > element.first) {
      return i;
    } else if(flat[i] == element.first && element.second <= localStart + i) {
      return i;
    }
  }

  //logger << me << ": finding " << element << ", did not find, using " << flat.size() << endl;
  //cout << logger.str(); logger.str("");

  return flat.size();
}

//Data, my rank, pe count, size of target partitions.
vector<size_t> partition(const vector<vector<T>> &data, int i, int p, size_t targetSize) {
  //logger << me << ": Process " << i << " doing partition." << endl; 
  //cout << logger.str(); logger.str("");
  //
  int iterations = 0;

  vector<size_t> parts(p - 1);
  
  srand(0); //Cheat to get the same randon numbers at all times. 

  size_t lsize = 0;
  for(auto part : data) {
    lsize += part.size();
  }

  //logger << me << ": Partitioning starting at " << i << " of " << p << " datalen: " << lsize << endl;
  //cout << logger.str(); logger.str("");

  //Holds left borders of partitions and their exclusive lengths. 
  size_t left[p - 1];
  std::fill(left, left + (p - 1), 0); 
  size_t rsize[p - 1]; 
  std::fill(rsize, rsize + (p - 1), lsize);
  size_t preDataSize; 

  //Take care. It would be better to do this by partition - in case we really want to do quick-select. 
  MPI_Scan(&lsize, &preDataSize, p, MPI_LONG, MPI::SUM, MPI_COMM_WORLD);
  
  //logArray(left, p - 1, "left");
  //logArray(rsize, p - 1, "rsize");

  //Aux arrays
  size_t rsizescan[p - 1]; 
  size_t rsizesum[p - 1]; 
 
  //srank - holds ranks we search for. 
  size_t srank[p - 1];

  for(size_t r = 0; r < p - 1; r++) 
    srank[r] = (r + 1) * targetSize;


  //logArray(srank, p - 1, "srank");
 
  while(1) {
    
      iterations++;

    MPI_Scan(rsize, rsizescan, p - 1, MPI_LONG, MPI::SUM, MPI_COMM_WORLD);
  
    if(i == p - 1) {
      memcpy(rsizesum, rsizescan, sizeof(size_t) * (p - 1));
    }  

    MPI_Bcast(rsizesum, p - 1, MPI_LONG, p - 1, MPI_COMM_WORLD);
    //logArray(rsizesum, p - 1, "rsizesum");

unsigned int done = 0;

for(size_t r = 0; r < p - 1; r++) {
  if(rsizesum[r] <= p) {
    parts[r] = left[r];
    done++;
    continue;
  }
}
if(done == p - 1) break;

    size_t pivotrank[p - 1];

    for(size_t r = 0; r < p - 1; r++) {
      if(rsizesum[r] > 1) {
        //need to make rand synchronous. 
        pivotrank[r] = rand() % rsizesum[r];   
      } else {
        pivotrank[r] = 0;
      }
    }

    //logArray(pivotrank, p - 1, "Pivotrank");

    vector<Pivot> pivot(p - 1);

    //Exchange Pivots
    
    for(size_t r = 0; r < p - 1; r++) {
      rsizescan[r] -= rsize[r]; // reduce to exclusive prefix sum


      if(rsizesum[r] > 1 && 
        rsizescan[r] <= pivotrank[r] &&
        pivotrank[r] < rsizescan[r] + rsize[r]) {
       
        //Rank of the pivor, in local index 
        size_t localRank = left[r] + pivotrank[r] - rsizescan[r];
        T pe = select(data, localRank, lsize);

        //Global pivot index.  
        Pivot p = Pivot(pe, localRank + preDataSize);  

        pivot[r] = p;  
      } else {
        pivot[r] = Pivot(0, 0);
      }
    }

    MPI_Allreduce(MPI_IN_PLACE, pivot.data(), (p - 1) * sizeof(Pivot), MPI::BYTE, MPI::BOR, MPI_COMM_WORLD);

    //logArray((Pivot*)pivot.data(), p - 1, "Pivots");

    //Find splitters depending on pivots. 
    size_t split[p - 1];

    for(size_t r = 0; r < p - 1; r++) {
      if(rsizesum[r] <= 1) {
        split[r] = 0;
      } else {
        split[r] = find(data, pivot[r], preDataSize) - left[r];
        //logger << me << ": Found pivot with value (" << pivot[r].first << ", " << pivot[r].second << ") at " << split[r] << endl;
        //cout << logger.str(); logger.str("");
      }
    } 
    //logArray(split, p - 1, "split");

    size_t splitsum[p - 1];
    MPI_Allreduce(split, splitsum, p - 1, MPI::LONG, MPI::SUM, MPI_COMM_WORLD);

    //logArray(splitsum, p - 1, "splitsum");

    //Recursion step. 
    for(size_t r = 0; r < p - 1; r++) {
      if (rsizesum[r] < 1) continue;

      if (splitsum[r] < srank[r]) // recurse into right part
      {
        left[r] += split[r];
        rsize[r] -= split[r];
        srank[r] -= splitsum[r];
      }
      else // recurse into left part
      {
        rsize[r] = split[r];
      }
    }

    //logArray(left, p - 1, "left");
    //logArray(rsize, p - 1, "rsize");
  }
    
  logger << me << ": Finished after " << iterations << " iterations." << endl;
  cout << logger.str(); logger.str("");

  return parts;
}


int main(int argc, char **argv) {
  
  int p, i;
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &i);
  MPI_Comm_size(MPI_COMM_WORLD, &p);
  me = i;

  auto data = createTestEnvironment();

  //logger << me << ": Process " << i << " picking data: " << endl;

  //for(auto part : data) {
  //  for(auto elem : part) {
  //    logger << elem << "\t";
  //  }
  //  logger << endl;
  //} 
  //cout << logger.str(); logger.str("");

  vector<size_t> partitions = partition(data, i, p, data_count * dim);

  //logArray((size_t*)partitions.data(), partitions.size(), "partitions");

  partitions.push_back(data_count * dim);

  for(int i = partitions.size() - 1; i > 0; i--) {
    partitions[i] -= partitions[i - 1];
  }
  //logArray((size_t*)partitions.data(), partitions.size(), "localSize");

  MPI_Allreduce(MPI_IN_PLACE, partitions.data(), p, MPI::LONG, MPI::SUM, MPI_COMM_WORLD);


  logArray((size_t*)partitions.data(), partitions.size(), "resultSize");

  MPI_Finalize();
  return 0;
}


