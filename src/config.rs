use corn::from_str;
use serde::Deserialize;
use std::fs::read_to_string;

#[derive(Deserialize)]
pub(crate) struct Config {
    pub schema: Schema,
}

#[derive(Deserialize)]
pub(crate) struct Schema {
    pub volatiles: Vec<String>,
    pub primary_key: Vec<String>,
}

pub fn get_config() -> Config {
    let file = read_to_string("config.corn").unwrap();
    from_str::<Config>(file.as_str()).unwrap()
}
