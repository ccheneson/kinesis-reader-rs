use anyhow::Result;
use anyhow;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ChainProvider;
use rusoto_kinesis::{DescribeStreamInput, DescribeStreamOutput, GetRecordsInput, GetShardIteratorInput, Kinesis, KinesisClient, Record, Shard};
use serde_json;

//Hold the next shard iterator ID to request
struct KinesisReaderShardIterId {
    shard_iterator_id: Option<String>
}


impl KinesisReaderShardIterId {
    //Get and return the records from the shard iterator ID and set the next one
    async fn get_records(&mut self, kinesis_client: &KinesisClient) -> Result<Vec<Record>> {
        match &self.shard_iterator_id {
            Some(shard_it_id) => {
                let records_input = GetRecordsInput {
                    limit: None,
                    shard_iterator: shard_it_id.to_string(),
                };
                let records_request = kinesis_client.get_records(records_input).await?;
                self.shard_iterator_id = records_request.next_shard_iterator;
                Ok(records_request.records)
            }
            None => {
                Ok(Vec::new())
            }
        }
    }
}


#[tokio::main]
async fn main() -> Result<()> {
    let kinesis_stream = std::env::args().nth(1);
    if kinesis_stream.is_none() {
        return Err(anyhow::anyhow!("Missing kinesis stream name"));
    }

    let kinesis_stream = kinesis_stream.unwrap();

    let credentials_provider = ChainProvider::new();
    let http_client = HttpClient::new().unwrap();
    let kinesis_client =
        KinesisClient::new_with(http_client, credentials_provider, Region::EuWest1);

    let stream = describe_kinesis(kinesis_stream.clone(), &kinesis_client).await?;
    let mut record_getters: Vec<KinesisReaderShardIterId> = get_shard_ids(kinesis_stream.clone(), stream.stream_description.shards, &kinesis_client).await?;

    loop {
        for record_getter in record_getters.iter_mut() {
            let records = record_getter.get_records(&kinesis_client).await?;
            for record in records {
                let json: serde_json::value::Value = serde_json::from_slice(&record.data)?;
                println!("--\n{}", serde_json::to_string_pretty(&json)?);
            };
        }
    };
}


async fn get_shard_ids(kinesis_stream: String, shards: Vec<Shard>, kinesis_client: &KinesisClient) -> Result<Vec<KinesisReaderShardIterId>> {
    let mut record_getters: Vec<KinesisReaderShardIterId> = Vec::new();

    for shard in shards {
        let shard_it_input = GetShardIteratorInput {
            shard_id: shard.shard_id,
            shard_iterator_type: "LATEST".to_owned(),
            starting_sequence_number: None,
            stream_name: kinesis_stream.to_owned(),
            timestamp: None,
        };

        let shard_it = kinesis_client.get_shard_iterator(shard_it_input).await?;

        let kinesis_reader_iterator = KinesisReaderShardIterId {
            shard_iterator_id: shard_it.shard_iterator
        };
        record_getters.push(kinesis_reader_iterator);
    };

    Ok(record_getters)
}


async fn describe_kinesis(kinesis_stream: String, kinesis_client: &KinesisClient) -> Result<DescribeStreamOutput> {
    let ds_input = DescribeStreamInput {
        stream_name: kinesis_stream,
        exclusive_start_shard_id: None,
        limit: None,
    };
    let description = kinesis_client.describe_stream(ds_input).await?;
    Ok(description)
}
