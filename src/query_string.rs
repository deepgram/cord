use failure::Error;
use serde_urlencoded;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

#[derive(Default, Debug, Clone)]
pub struct QueryString {
    pairs: HashMap<String, String>,
}

impl FromStr for QueryString {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            pairs: serde_urlencoded::from_str::<HashMap<String, String>>(s)?,
        })
    }
}

impl QueryString {
    pub fn to_str(&self) -> Result<String, Error> {
        Ok(serde_urlencoded::to_string(&self.pairs)?)
    }

    pub fn push<T>(&mut self, key: &str, val: &T)
    where
        T: fmt::Display,
    {
        self.pairs.insert(key.to_string(), format!("{}", val));
    }
}
