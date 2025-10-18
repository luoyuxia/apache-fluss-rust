/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::thread::sleep;
use std::time::Duration;
use tokio::try_join;
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::GenericRow;

#[tokio::test]
async fn append_data_table() {
    let mut config = Config::default();
    config.writer_acks = "all".to_string();
    config.bootstrap_server = Some("127.0.0.1:9123".to_string());

    let connection = FlussConnection::new(config).await.expect("Failed to connect to Fluss");


    let table_path = TablePath::new(
        "fluss".to_string(),
        "duck_table".to_string());
    

    // write row
    let mut row = GenericRow::new();
    row.set_field(0, 3333);
    row.set_field(1, "t2t");

    let table = connection.get_table(&table_path).await.expect("Failed to get table");
    let append_writer = table.new_append().expect("fail to new append").create_writer();
    let f1 = append_writer.append(row);
    row = GenericRow::new();
    row.set_field(0, 4444);
    row.set_field(1, "tt44");
    let f2 = append_writer.append(row);
    try_join!(f1, f2, append_writer.flush()).expect("Failed to flush table");

    // sleep(Duration::from_secs(1000000));
}