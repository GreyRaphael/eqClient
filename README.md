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