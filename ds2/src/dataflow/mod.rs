// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

pub extern crate petgraph;

pub mod topology;
pub mod parse;

pub type Timestamp = u64;
pub type Epoch = u64;
pub type OperatorId = String;
pub type ChannelId = String;
pub type WorkerId = String;
pub type OperatorInstanceId = String;
pub type OperatorInstances = u32;
pub type Rate = f64;
pub type Log = (Timestamp,Rate,Rate,Rate,Rate);
pub type Rates = (Rate,Rate,Rate,Rate);