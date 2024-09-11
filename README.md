# README

- [README](#readme)
  - [Usage](#usage)
  - [Calendar](#calendar)
  - [Tips](#tips)
  - [order \& trade](#order--trade)

## Usage

calendar is download from windapi table

## Calendar

- 2007 242
- 2008 246
- 2009 244
- 2010 242
- 2011 244
- 2012 243
- 2013 238
- 2014 245
- 2015 244
- 2016 244
- 2017 244
- 2018 243
- 2019 244
- 2020 243
- 2021 243
- 2022 242
- 2023 242

## Tips

async method: `get()` 和 `wait()` 一一对应

如果没有找到quotes, `onQuote()`不会被调用

## order & trade

order sh
- securityid √
<!-- - market -->
- date √
- time √
<!-- - quote_type -->
<!-- - eq_trading_phase_code -->
- biz_index ? always == order_index ?
<!-- - trade_order_channel -->
- order_index √
- order_price √
- order_volume √
- order_side √
- order_type √
- order_origin_no ?

order sz
- securityid √
<!-- - market -->
- date √
- time √
<!-- - quote_type -->
<!-- - trade_order_channel -->
- order_index √
- order_price √
- order_volume √
- order_side √
- order_type √
- no order_origin_no field, aiquant treat as 0

order aiquant
- instrument
- date
- bs_flag
- order_type
- price
- volume
- seq_num
- original_seq_num

trade sh
- securityid √
<!-- - market -->
- date √
- time √
<!-- - quote_type -->
- trade_index √
- trade_price √
- trade_volume √
- trade_bs_flag √
- trade_sell_no √
- trade_buy_no √
- biz_index: ?
<!-- - trade_order_channel -->

trade sz
- securityid √
<!-- - market -->
- date √
- time √
<!-- - quote_type -->
- trade_index √
- trade_price √
- trade_volume √
- trade_bs_flag √
- trade_sell_no √
- trade_buy_no √
<!-- - trade_order_channel -->

trade aiquant
- instrument
- date
- bs_flag
- price
- volume
- seq_num
- ask_seq_num
- bid_seq_num

eqapi

```cpp
struct Trade
{
  char security_id[16];
  std::uint16_t market;
  std::int32_t date;
  std::int32_t time;
  std::uint8_t quote_type;
  std::int8_t eq_tpc;                        // eqapi trading phase code
  std::int64_t index;
  std::int64_t price;
  std::int64_t volume;
  char bsFlag;
  std::int64_t sellNo;
  std::int64_t buyNo;
  std::int16_t type;                          // 港股成交类型
  std::int32_t channel;
  std::int64_t biz_index;                     // sh biz index
};
struct Order
{
  char security_id[16];
  std::uint16_t market;
  std::int32_t date;
  std::int32_t time;
  std::uint8_t quote_type;
  std::int8_t eq_tpc;                        // eqapi trading phase code
  std::int64_t index;
  std::int64_t price;
  std::int64_t volume;
  char side;
  char type;
  std::int32_t channel;
  std::int64_t origin_no;                    // for sh, 原始订单号
  std::int64_t biz_index;                    // for sh, biz index
};

struct Snapshot_L2
{
  char security_id[16];
  std::uint16_t market;
  std::int32_t date;
  std::int32_t time;
  std::uint8_t quote_type;
  std::int8_t eq_tpc;                        // eqapi trading phase code
  std::int64_t preclose;
  std::int64_t open;
  std::int64_t high;
  std::int64_t low;
  std::int64_t last;
  std::int64_t close;
  char instrument_status[8];
  char tpc[8];
  std::int64_t offer_price[10];
  std::int64_t offer_volume[10];
  std::int64_t offer_num_order[10];
  std::int64_t bid_price[10];
  std::int64_t bid_volume[10];
  std::int64_t bid_num_order[10];
  std::int64_t num_trades;
  std::int64_t total_volume_trade;
  std::int64_t total_value_trade;
  std::int64_t total_offer_quant;
  std::int64_t total_bid_quant;                   // 买入总量
  std::int64_t weighted_avg_offer_price;
  std::int64_t weighted_avg_bid_price;
  std::int64_t altWeighted_avg_offer_price;
  std::int64_t altWeighted_avg_bid_price;
  std::int64_t num_offer_order;
  std::int64_t num_bid_order;                     // 买方委托价位数
  std::int64_t high_limited;
  std::int64_t low_limited;
  std::int64_t withdraw_sell_num;              //期权集中竞价交易熔断参考价格 （暂用）
  std::int64_t withdraw_sell_amount;
  std::int64_t withdraw_sell_money;              
  std::int64_t withdraw_buy_num;       
  std::int64_t withdraw_buy_amount;
  std::int64_t withdraw_buy_money;
  std::int64_t total_offer_num;
  std::int64_t total_bid_num;                     // 买入总笔数
  std::int64_t offer_trade_max_duration;
  std::int64_t bid_trade_max_duration;
  std::int64_t etf_buy_num;
  std::int64_t etf_buy_amount;
  std::int64_t etf_buy_money;
  std::int64_t etf_sell_num;
  std::int64_t etf_sell_amount;
  std::int64_t etf_sell_money;
  std::int64_t iopv;
  std::int64_t nav;
  std::int64_t position;
  std::int64_t yield_to_maturity;
  std::int64_t pratio1;
  std::int64_t pratio2;
  std::int64_t updown1;
  std::int64_t updown2;
  std::int64_t weighted_avg_price;                // 上交所债券加权平均回购利率  & 深交所债券现券交易成交量加权平均价
  std::int64_t reserved2;                         // 匹配成交最新价
  std::int64_t reserved3;                         // 匹配成交成交量
  std::int64_t reserved4;                         // 匹配成交成交金额
  std::int32_t offer_one_order[50];
  std::int32_t bid_one_order[50];
};

struct Kline
{
  char security_id[16];
  std::uint16_t market;
  std::int32_t date;
  std::int32_t time;
  std::uint8_t quote_type;
  std::int64_t preclose;
  std::int64_t open;
  std::int64_t high;
  std::int64_t low;
  std::int64_t last;
  std::int64_t numTrade;
  std::int64_t volume;
  std::int64_t value;
  std::int64_t position;
};
```

