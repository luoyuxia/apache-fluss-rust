// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::integration::fluss_cluster::FlussTestingCluster;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::sync::Arc;

#[cfg(test)]
use test_env_helpers::*;

// Module-level shared cluster instance (only for this test file)
static SHARED_FLUSS_CLUSTER: Lazy<Arc<RwLock<Option<FlussTestingCluster>>>> =
    Lazy::new(|| Arc::new(RwLock::new(None)));

#[cfg(test)]
#[before_all]
#[after_all]
mod admin_test {
    use super::SHARED_FLUSS_CLUSTER;
    use crate::integration::fluss_cluster::{FlussTestingCluster, FlussTestingClusterBuilder};
    use fluss::metadata::{
        DataTypes, DatabaseDescriptorBuilder, KvFormat, LogFormat, Schema, TableDescriptor,
        TablePath,
    };
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;
    use tokio::try_join;
    use fluss::row::GenericRow;

    fn before_all() {
        // Create a new tokio runtime in a separate thread
        let cluster_guard = SHARED_FLUSS_CLUSTER.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
            rt.block_on(async {
                let cluster = FlussTestingClusterBuilder::new().build().await;
                let mut guard = cluster_guard.write();
                *guard = Some(cluster);
            });
        })
            .join()
            .expect("Failed to create cluster");
    }

    fn get_fluss_cluster() -> Arc<FlussTestingCluster> {
        let cluster_guard = SHARED_FLUSS_CLUSTER.read();
        if cluster_guard.is_none() {
            panic!("Fluss cluster not initialized. Make sure before_all() was called.");
        }
        Arc::new(cluster_guard.as_ref().unwrap().clone())
    }

    fn after_all() {
        // Create a new tokio runtime in a separate thread
        let cluster_guard = SHARED_FLUSS_CLUSTER.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
            rt.block_on(async {
                let mut guard = cluster_guard.write();
                if let Some(cluster) = guard.take() {
                    cluster.stop().await;
                }
            });
        })
            .join()
            .expect("Failed to cleanup cluster");
    }

    #[tokio::test]
    async fn test_create_database() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection
            .get_admin()
            .await
            .expect("Failed to get admin client");


        let table_path = TablePath::new(
            "fluss".to_string(), 
            "duck_table".to_string());

        // build table schema
        let table_schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .build()
            .expect("Failed to build table schema");

        // build table descriptor
        let table_descriptor = TableDescriptor::builder()
            .schema(table_schema.clone())
            .build()
            .expect("Failed to build table descriptor");

        // create test table
        admin
            .create_table(&table_path, &table_descriptor, false)
            .await
            .expect("Failed to create test table");
        

        // write row
        let mut row = GenericRow::new();
        row.set_field(0, 22222);
        row.set_field(1, "t2t");

        let table = connection.get_table(&table_path).await.expect("Failed to get table");
        let append_writer = table.new_append().expect("fail to new append").create_writer();
        let f1 = append_writer.append(row);
        row = GenericRow::new();
        row.set_field(0, 233333);
        row.set_field(1, "tt44");
        let f2 = append_writer.append(row);
        try_join!(f1, f2, append_writer.flush()).expect("Failed to flush table");
        
        sleep(Duration::from_secs(1000000));
    }
}
