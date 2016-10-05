/*******************************************************************************
 * examples/tpch/tpch_run.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/page_rank/zipf_graph_gen.hpp>
#include <examples/triangles/triangles.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/join.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/size.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/common/string.hpp>

#include <algorithm>
#include <cmath>
#include <ctime>
#include <functional>
#include <utility>
#include <vector>

using namespace thrill;              // NOLINT

struct LineItem {
    size_t orderkey;
    size_t partkey;
    size_t suppkey;
    size_t linenumber;
    size_t quantity;
    double extendedprice;
    double discount;
    double tax;
    char   returnflag;
    char   linestatus;
    time_t ship;
    time_t commit;
    time_t receipt;
    char   shipinstruct[25];
    char   shipmode[10];
    char   comment[44];
} THRILL_ATTRIBUTE_PACKED;

struct Order {
    size_t orderkey;
    size_t custkey;
    char   orderstatus;
    double totalprice;
    time_t ordertime;
    char   orderpriority[16];
    char   clerk[16];
    bool   priority;
    char   comment[79];

    friend std::ostream& operator << (std::ostream& os, const Order& o) {
        os << '(' << o.orderpriority << "|" << o.clerk << "|" << o.comment;
        return os << ')';
    }
} THRILL_ATTRIBUTE_PACKED;

struct JoinedElement {
    size_t orderkey;
    size_t partkey;
    size_t suppkey;
    size_t linenumber;
    size_t quantity;
    double extendedprice;
    double discount;
    double tax;
    char   returnflag;
    char   linestatus;
    time_t ship;
    time_t commit;
    time_t receipt;
    char   shipinstruct[25];
    char   shipmode[10];
    char   lineitem_comment[44];
    size_t custkey;
    char   orderstatus;
    double totalprice;
    time_t ordertime;
    char   orderpriority[16];
    char   clerk[16];
    bool   priority;
    char   order_comment[79];
} THRILL_ATTRIBUTE_PACKED;

JoinedElement ConstructJoinedElement(const struct LineItem& li, const struct Order& o) {
    JoinedElement je;
    je.orderkey = li.orderkey;
    je.partkey = li.partkey;
    je.suppkey = li.suppkey;
    je.linenumber = li.linenumber;
    je.quantity = li.quantity;
    je.extendedprice = li.extendedprice;
    je.discount = li.discount;
    je.tax = li.tax;
    je.returnflag = li.returnflag;
    je.linestatus = li.linestatus;
    je.ship = li.ship;
    je.commit = li.commit;
    je.receipt = li.receipt;
    std::strcpy(je.shipinstruct, li.shipinstruct);
    std::strcpy(je.shipmode, li.shipmode);
    std::strcpy(je.lineitem_comment, li.comment);
    je.custkey = o.custkey;
    je.orderstatus = o.orderstatus;
    je.totalprice = o.totalprice;
    je.ordertime = o.ordertime;
    std::strcpy(je.orderpriority, o.orderpriority);
    std::strcpy(je.clerk, o.clerk);
    je.priority = o.priority;
    std::strcpy(je.order_comment, o.comment);
    return je;
}

// adapted from:
// https://gmbabar.wordpress.com/2010/12/01/mktime-slow-use-custom-function/
// removed time of day as we dont need that
time_t time_to_epoch(const std::string& str) {
    char* end;

    struct tm ltm;
    ltm.tm_year = std::strtoul(str.substr(0, 4).c_str(),
                               &end, 10) - 1900;
    ltm.tm_mon = std::strtoul(str.substr(5, 2).c_str(),
                              &end, 10);
    ltm.tm_mday = std::strtoul(str.substr(8).c_str(),
                               &end, 10);

    const int mon_days[] =
    { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    long tyears, tdays, leaps;
    int i;

    tyears = ltm.tm_year - 70; // tm->tm_year is from 1900.
    leaps = (tyears + 2) / 4;  // no of next two lines until year 2100.
    // i = (ltm->tm_year – 100) / 100;
    // leaps -= ( (i/4)*3 + i%4 );
    tdays = 0;
    for (i = 0; i < ltm.tm_mon; i++) tdays += mon_days[i];

    tdays += ltm.tm_mday - 1; // days of month passed.
    tdays = tdays + (tyears * 365) + leaps;

    return (tdays * 86400);
}

static size_t JoinTPCH4(
    api::Context& ctx,
    const std::vector<std::string>& input_path) {
    ctx.enable_consume();
    std::vector<std::string> splitted;
    splitted.resize(17);
    std::string s_lineitems = input_path[0] + std::string("lineitem");

    auto lineitems = ReadLines(ctx, s_lineitems).FlatMap<struct LineItem>(
        [&splitted](const std::string& input, auto emit) {

            LineItem li;
            char* end;
            common::SplitRef(input, '|', splitted);

            li.commit = time_to_epoch(splitted[11]);
            li.receipt = time_to_epoch(splitted[12]);

            if (li.commit < li.receipt) {

                li.orderkey = std::strtoul(splitted[0].c_str(), &end, 10);
                li.partkey = std::strtoul(splitted[1].c_str(), &end, 10);
                li.suppkey = std::strtoul(splitted[2].c_str(), &end, 10);
                li.linenumber = std::strtoul(splitted[3].c_str(), &end, 10);
                li.quantity = std::strtoul(splitted[4].c_str(), &end, 10);
                li.extendedprice = std::strtod(splitted[5].c_str(), &end);
                li.discount = std::strtod(splitted[6].c_str(), &end);
                li.tax = std::strtod(splitted[7].c_str(), &end);
                li.returnflag = splitted[8][0];
                li.linestatus = splitted[9][0];

                li.ship = time_to_epoch(splitted[10]);

                std::strcpy(li.shipinstruct, splitted[13].data());
                std::strcpy(li.shipmode, splitted[14].data());
                std::strcpy(li.comment, splitted[15].data());

                emit(li);
            }
        }).Cache().Execute();

    time_t starttime = time_to_epoch("1993-07-01");
    time_t stoptime = time_to_epoch("1993-10-01");

    std::string s_orders = input_path[0] + std::string("orders");
    auto orders = ReadLines(ctx, s_orders).FlatMap<struct Order>(
        [&splitted, &starttime, &stoptime](const std::string& input, auto emit) {
            Order o;

            char* end;
            common::SplitRef(input, '|', splitted);

            o.ordertime = time_to_epoch(splitted[4]);
            if (o.ordertime >= starttime && o.ordertime < stoptime) {
                o.orderkey = std::strtoul(splitted[0].c_str(), &end, 10);
                o.custkey = std::strtoul(splitted[1].c_str(), &end, 10);
                o.orderstatus = splitted[2][0];
                o.totalprice = std::strtod(splitted[3].c_str(), &end);
                std::strcpy(o.orderpriority, splitted[5].data());
                std::strcpy(o.clerk, splitted[6].data());
                o.priority = (splitted[7][0] != '0');
                std::strcpy(o.comment, splitted[8].data());

                emit(o);
            }
        }).Cache().Execute();

    ctx.net.Barrier();

    common::StatsTimerStart timer;

    auto joined = lineitems.InnerJoinWith(orders,
                                          [](const LineItem& li) {
                                              return li.orderkey;
                                          },
                                          [](const Order& o) {
                                              return o.orderkey;
                                          },
                                          [](const LineItem& li, const Order& o) {
                                              return ConstructJoinedElement(li, o);
                                          }, thrill::hash()).Size();


    ctx.net.Barrier();

    if (ctx.my_rank() == 0) {
        LOG1 << "RESULT " << "detection=OFF " << "time=" << timer.Milliseconds()
             << " machines=" << ctx.num_hosts();
}

    return joined;
}

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    std::vector<std::string> input_path;
    clp.AddParamStringlist("input", input_path,
                           "input file pattern");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    die_unless(input_path.size() == 1);

    clp.PrintResult();

    return api::Run(
        [&](api::Context& ctx) {
            ctx.enable_consume();

            LOG1 << JoinTPCH4(ctx, input_path);

            return 42;
        });
}

/******************************************************************************/
