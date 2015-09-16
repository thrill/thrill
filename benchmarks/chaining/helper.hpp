struct KeyValue {
    size_t key;
    size_t value;
};

#define map Map([](const KeyValue& elem) { return KeyValue{elem.key, elem.value + 1}; })
#define map10 Map([](const KeyValue& elem) { size_t value = elem.value; value += 1; value +=1; value +=1; value +=1; value +=1; value +=1; value +=1; value +=1; value +=1; value +=1; return KeyValue{elem.key, value}; })
