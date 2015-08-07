from sys import stdin

def second_word(words):
    return words.split(' ')[2]
def third_word(words):
    return words.split(' ')[3]

def parse_line(s):
    splits = s.replace('\n', '').split(';')
    tmp = splits[0][1:].split(' ')
    host_worker_str = tmp[0] + ' ' + tmp[1] + ' ' + tmp[2] + ' ' + tmp[3]
    channel_id  = splits[0].split(' ')[7]
    bytes_in    = second_word(splits[1])
    blocks_in   = second_word(splits[2])
    bytes_out   = second_word(splits[3])
    blocks_out  = second_word(splits[4])
    tx_lifetime = third_word(splits[5])
    rx_lifetime = third_word(splits[6])
    tx_timespan = third_word(splits[7])
    rx_timespan = third_word(splits[8])

    return (host_worker_str, channel_id, bytes_in, blocks_in, bytes_out, blocks_out, rx_lifetime, tx_lifetime, rx_timespan, tx_timespan)

us_to_ms = 1.0 / 1000.0

def enrich_data(data_point):
    bandwidth_in_gross = float(data_point[2]) / (float(data_point[6]) * us_to_ms)
    bandwidth_out_gross = float(data_point[2]) / (float(data_point[7]) * us_to_ms)
    bandwidth_in = float(data_point[2]) / (float(data_point[8]) * us_to_ms)
    bandwidth_out = float(data_point[2]) / (float(data_point[9]) * us_to_ms)
    return tuple(list(data_point) + [bandwidth_in_gross, bandwidth_out_gross, bandwidth_in, bandwidth_out])

def print_header():
    print "host_worker; channel_id; bytes_in; blocks_in; bytes_out; blocks_out; rx_lifetime; tx_timetime; rx_timespan; tx_timespam; bandwidth_in_gross; bandwidth_out_gross; bandwidth_in; bandwidth_out\n"

def print_data(data_point):
    as_list = list(data_point)
    print "; ".join(map(str, as_list))

print_header()
while True:
    line = stdin.readline()
    if not line:
        break
    if not "[Network] channel " in line: continue
    print_data(enrich_data(parse_line(line)))
