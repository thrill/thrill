/*******************************************************************************
 * examples/triangles/triangles_run.cpp
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
using LineItem = std::tuple<size_t, size_t, size_t, size_t, size_t, double,
                            double, double, char, char, time_t, time_t, time_t,
                            std::string, std::string, std::string>;
using Order = std::tuple<size_t, size_t, char, double, time_t, std::string,
                         std::string, bool, std::string>;
using Joined = std::tuple<size_t, size_t, size_t, size_t, size_t, double,
                          double, double, char, char, time_t, time_t, time_t,
                          std::string, std::string, std::string, size_t, size_t,
                          char, double, time_t, std::string, std::string, bool,
                          std::string>;
//from: https://gmbabar.wordpress.com/2010/12/01/mktime-slow-use-custom-function/, removed hours as we dont need that
time_t time_to_epoch ( const struct tm *ltm) {
   const int mon_days [] =
      {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
   long tyears, tdays, leaps;
   int i;

   tyears = ltm->tm_year - 70; // tm->tm_year is from 1900.
   leaps = (tyears + 2) / 4; // no of next two lines until year 2100.
   //i = (ltm->tm_year – 100) / 100;
   //leaps -= ( (i/4)*3 + i%4 );
   tdays = 0;
   for (i=0; i < ltm->tm_mon; i++) tdays += mon_days[i];

   tdays += ltm->tm_mday-1; // days of month passed.
   tdays = tdays + (tyears * 365) + leaps;

   return (tdays * 86400);
}

static size_t JoinTPCH4(
    api::Context& ctx,
    const std::vector<std::string>& input_path) {
    ctx.enable_consume();
    common::StatsTimerStopped sts;
    common::StatsTimerStopped notime;
    std::string s_lineitems = input_path[0] + std::string("lineitem.tbl*");
    std::vector<std::string> splitted;
    auto lineitems = ReadLines(ctx, s_lineitems).Map(
        [&sts, &notime, &splitted](const std::string& input) {
            notime.Start();
            char* end;

            common::SplitRef(input, '|', splitted);
            size_t orderkey = std::strtoul(splitted[0].c_str(), &end, 10);
            size_t partkey = std::strtoul(splitted[1].c_str(), &end, 10);
            size_t suppkey = std::strtoul(splitted[2].c_str(), &end, 10);
            size_t linenumber = std::strtoul(splitted[3].c_str(), &end, 10);
            size_t quantity = std::strtoul(splitted[4].c_str(), &end, 10);
            double extendedprice = std::strtod(splitted[5].c_str(), &end);
            double discount = std::strtod(splitted[6].c_str(), &end);
            double tax = std::strtod(splitted[7].c_str(), &end);
            char returnflag = splitted[8][0];
            char linestatus = splitted[9][0];

            notime.Stop();

            //make dates
            struct tm time_ship;
            time_ship.tm_year = std::strtoul(splitted[10].substr(0,4).c_str(),
                                               &end, 10) - 1900;
            time_ship.tm_mon = std::strtoul(splitted[10].substr(5,2).c_str(),
                                                &end, 10);
            time_ship.tm_mday = std::strtoul(splitted[10].substr(8).c_str(),
                                              &end, 10);
            struct tm time_cmt;
            time_cmt.tm_year = std::strtoul(splitted[11].substr(0,4).c_str(),
                                               &end, 10) - 1900;
            time_cmt.tm_mon = std::strtoul(splitted[11].substr(5,2).c_str(),
                                                &end, 10);
            time_cmt.tm_mday = std::strtoul(splitted[11].substr(8).c_str(),
                                              &end, 10);
            struct tm time_rcpt;
            time_rcpt.tm_year = std::strtoul(splitted[12].substr(0,4).c_str(),
                                               &end, 10) - 1900;
            time_rcpt.tm_mon = std::strtoul(splitted[12].substr(5,2).c_str(),
                                                &end, 10);
            time_rcpt.tm_mday = std::strtoul(splitted[12].substr(8).c_str(),
                                              &end, 10);

            sts.Start();

            time_t ship = time_to_epoch(&time_ship);
            time_t commit = time_to_epoch(&time_cmt);
            time_t receipt = time_to_epoch(&time_rcpt);

            sts.Stop();

            return std::make_tuple(orderkey, partkey, suppkey, linenumber,
                                   quantity, extendedprice, discount, tax,
                                   returnflag, linestatus, ship, commit,
                                   receipt, splitted[13], splitted[14],
                                   splitted[15]);

        }).Filter([](const LineItem& li) {
                return std::get<11>(li) < std::get<12>(li);
            });

    struct tm starttimestr;
    starttimestr.tm_year = 92;
    starttimestr.tm_mon = 1;
    starttimestr.tm_mday = 1;
    struct tm stoptimestr;
    stoptimestr.tm_year = 92;
    stoptimestr.tm_mon = 4;
    stoptimestr.tm_mday = 1;
    time_t starttime = time_to_epoch(&starttimestr);
    time_t stoptime = time_to_epoch(&stoptimestr);

    common::StatsTimerStopped sts2;
    std::string s_orders = input_path[0] + std::string("orders.tbl*");
    auto orders = ReadLines(ctx, s_orders).Map(
        [&sts2](const std::string& input) {

            sts2.Start();
            char* end;
            std::vector<std::string> splitted = common::Split(input, '|');
            size_t orderkey = std::strtoul(splitted[0].c_str(), &end, 10);
            size_t custkey = std::strtoul(splitted[1].c_str(), &end, 10);
            char orderstatus = splitted[2][0];
            double totalprice = std::strtod(splitted[3].c_str(), &end);


            struct tm time_order;
            time_order.tm_year = std::strtoul(splitted[4].substr(0,4).c_str(),
                                               &end, 10) - 1900;
            time_order.tm_mon = std::strtoul(splitted[4].substr(5,2).c_str(),
                                                &end, 10);
            time_order.tm_mday = std::strtoul(splitted[4].substr(8).c_str(),
                                              &end, 10);
            time_t order = time_to_epoch(&time_order);

            bool priority = (splitted[7][0] != '0');

            sts2.Stop();

            return std::make_tuple(orderkey, custkey, orderstatus, totalprice,
                                   order, splitted[5], splitted[6],
                                   priority, splitted[8]);

        }).Filter([&starttime](const Order& o) {
                return (std::get<4>(o) >= starttime);
            }).Filter([&stoptime](const Order& o) {
                    return (std::get<4>(o) < stoptime);
                });


    auto joined = lineitems.InnerJoinWith(orders,
                                          [](const LineItem& li) {
                                              return std::get<0>(li);
                                          },
                                          [](const Order& o) {
                                              return std::get<0>(o);
                                          },
                                          [](const LineItem& li, const Order& o) {
                                              return std::tuple_cat(li, o);
                                          });

    size_t size = joined.Size();

    LOG1 << sts.Milliseconds() << " to " << notime.Milliseconds() << " " << sts2.Milliseconds();
    return size;
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
