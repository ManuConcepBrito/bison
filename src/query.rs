use serde_json::{Map, Number, Value};
use std::fmt::Debug;
use std::str::FromStr;

#[derive(Debug)]
pub enum QueryOperator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanEqual,
    LessThan,
    LessThanEqual,
}

#[derive(Debug, PartialEq)]
pub enum UpdateOperator {
    Set,
    Add,
    Substract,
    Increment,
    Decrement,
    Delete,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Query<T> {
    fields: Vec<String>,
    value: Value,
    operator: T,
}


#[derive(Debug)]
pub struct QueryEngine<T> {
    queries: Vec<Query<T>>,
}

impl FromStr for QueryOperator {
    type Err = ();

    fn from_str(query_op: &str) -> Result<QueryOperator, Self::Err> {
        match query_op {
            "$eq" => Ok(QueryOperator::Equal),
            "$ne" => Ok(QueryOperator::NotEqual),
            "$gt" => Ok(QueryOperator::GreaterThan),
            "$gte" => Ok(QueryOperator::GreaterThanEqual),
            "$lt" => Ok(QueryOperator::LessThan),
            "$lte" => Ok(QueryOperator::LessThanEqual),
            _ => Err(()),
        }
    }
}

impl FromStr for UpdateOperator {
    type Err = ();

    fn from_str(query_op: &str) -> Result<UpdateOperator, Self::Err> {
        match query_op {
            "$set" => Ok(UpdateOperator::Set),
            "$add" => Ok(UpdateOperator::Add),
            "$substract" => Ok(UpdateOperator::Substract),
            "$inc" => Ok(UpdateOperator::Increment),
            "$dec" => Ok(UpdateOperator::Decrement),
            "$delete" => Ok(UpdateOperator::Delete),
            _ => Err(()),
        }
    }
}

impl Query<QueryOperator> {
    pub fn execute(&self, collection: &Map<String, Value>) -> bool {
        let mut current_value = collection;

        for key in &self.fields[..self.fields.len() - 1] {
            match current_value.get(key) {
                Some(Value::Object(map)) => {
                    current_value = map;
                }
                _ => {
                    return false;
                }
            }
        }
        let last_key = &self.fields[self.fields.len() - 1];
        match current_value.get(last_key) {
            Some(value) => self._execute_operator(value),
            None => false,
        }
    }
    pub fn _execute_operator(&self, last_value: &Value) -> bool {
        match self.operator {
            QueryOperator::Equal => &self.value == last_value,
            QueryOperator::NotEqual => &self.value != last_value,
            // TODO: The following should panic, an a comprehensible error
            // be sent to python
            QueryOperator::GreaterThan => {
                if let (Some(query_value), Some(found_value)) =
                    (self.value.as_number(), last_value.as_number())
                {
                    return found_value.clone().as_f64() > query_value.clone().as_f64();
                }
                false
            }

            QueryOperator::LessThan => {
                if let (Some(query_value), Some(found_value)) =
                    (self.value.as_number(), last_value.as_number())
                {
                    return found_value.clone().as_f64() < query_value.clone().as_f64();
                }
                false
            }

            QueryOperator::GreaterThanEqual => {
                if let (Some(query_value), Some(found_value)) =
                    (self.value.as_number(), last_value.as_number())
                {
                    return found_value.clone().as_f64() >= query_value.clone().as_f64();
                }
                false
            }
            QueryOperator::LessThanEqual => {
                if let (Some(query_value), Some(found_value)) =
                    (self.value.as_number(), last_value.as_number())
                {
                    return found_value.clone().as_f64() <= query_value.clone().as_f64();
                }
                false
            }
        }
    }
}

impl Query<UpdateOperator> {
    pub fn execute(&self, collection: &mut Map<String, Value>) -> bool {
        let mut current_value = collection;

        for key in &self.fields[..self.fields.len() - 1] {
            match current_value.get_mut(key) {
                Some(Value::Object(map)) => {
                    current_value = map;
                }
                _ => {
                    return false;
                }
            }
        }
        let last_key = &self.fields[self.fields.len() - 1];
        // handle delete operator
        if self.operator == UpdateOperator::Delete {
            let _ = current_value.remove_entry(last_key);
        } else {
            match current_value.get_mut(last_key) {
                Some(value) => self._execute_operator(value),
                None => {}
            }
        }
        return false;
    }
    pub fn _execute_operator(&self, last_value: &mut Value) -> () {
        match self.operator {
            UpdateOperator::Set => *last_value = self.value.clone(),
            UpdateOperator::Increment => {
                if let Some(found_value) = last_value.as_number() {
                    let mut numerical_value = found_value.clone().as_f64().unwrap();
                    numerical_value += 1.0;
                    *last_value = Number::from_f64(numerical_value).into()
                }
            }
            UpdateOperator::Decrement => {
                if let Some(found_value) = last_value.as_number() {
                    let mut numerical_value = found_value.clone().as_f64().unwrap();
                    numerical_value -= 1.0;
                    *last_value = Number::from_f64(numerical_value).into()
                }
            }
            UpdateOperator::Add => {
                if let (Some(query_value), Some(found_value)) =
                    (self.value.as_number(), last_value.as_number())
                {
                    let found_value = found_value.clone().as_f64().unwrap();
                    let query_value = query_value.clone().as_f64().unwrap();
                    let result = found_value + query_value;
                    *last_value = Number::from_f64(result).into();
                }
            }

            UpdateOperator::Substract => {
                if let (Some(query_value), Some(found_value)) =
                    (self.value.as_number(), last_value.as_number())
                {
                    let found_value = found_value.clone().as_f64().unwrap();
                    let query_value = query_value.clone().as_f64().unwrap();
                    let result = found_value - query_value;
                    *last_value = Number::from_f64(result).into();
                }
            }

            // This operator needs to be handle at key level
            UpdateOperator::Delete => {}
        }
    }
}

impl QueryEngine<QueryOperator> {
    pub fn new(unparsed_query: &Map<String, Value>) -> Self {
        // Compile an unparsed query into a list of queries.
        //
        // # Examples
        //
        // let query_engine = QueryEngine({"a": 10}) // Query to search for a = 10
        // let query_engine = QueryEngine({"a": {"eq": 10}}) // Equivalent query
        // let query_engine = QueryEgine({"a": {"b": 100}}) // Nested query
        //
        let queries: Vec<Query<QueryOperator>> = unparsed_query
            .into_iter()
            .map(|(key, sub_query)| {
                let mut fields: Vec<String> = Vec::new();
                let value = parse_query(sub_query, key, &mut fields);
                // if no '$' operator is found, assume it is an EqualOperator
                // For example: {"a": 10} => a == 10
                let mut query_op = QueryOperator::Equal;
                if fields.last().unwrap().chars().next() == Some('$') {
                    let query_op_str = fields.pop().unwrap();
                    // TODO: Error should be a python error
                    query_op = QueryOperator::from_str(&query_op_str)
                        .expect(&format!("Unknown query operator found: {}", query_op_str));
                }
                return Query {
                    fields,
                    value,
                    operator: query_op,
                };
            })
            .collect();
        QueryEngine { queries }
    }

    pub fn execute(&self, collection: &Map<String, Value>) -> bool {
        let query_iter = self.queries.iter();
        for q in query_iter {
            let query_result = q.execute(collection);
            if !query_result {
                return false;
            }
        }
        return true;
    }
}

impl QueryEngine<UpdateOperator> {
    pub fn new(unparsed_query: &Map<String, Value>) -> Self {
        let queries: Vec<Query<UpdateOperator>> = unparsed_query
            .into_iter()
            .map(|(key, sub_query)| {
                let mut fields: Vec<String> = Vec::new();
                let value = parse_query(sub_query, key, &mut fields);
                // if no '$' operator is found, assume it is an SetOperator
                // For example: {"a": 10} => a == 10
                let mut update_op = UpdateOperator::Set;
                if fields.last().unwrap().chars().next() == Some('$') {
                    let update_op_str = fields.pop().unwrap();
                    // TODO: Error should be a python error
                    update_op = UpdateOperator::from_str(&update_op_str).expect(&format!(
                        "Unknown update query operator found: {}",
                        update_op_str
                    ));
                }
                println!("Found operator: {:?}", update_op);
                return Query {
                    fields,
                    value,
                    operator: update_op,
                };
            })
            .collect();
        QueryEngine { queries }
    }

    pub fn execute(&self, collection: &mut Map<String, Value>) -> () {
        let query_iter = self.queries.iter();
        for q in query_iter {
            q.execute(collection);
        }
    }
}

fn parse_query(sub_query: &Value, key: &str, fields: &mut Vec<String>) -> Value {
    /*
     * Parses a query recursively. It extracts the fields and value involved in a query.
     * When there is a nested query like {"a": {"b": 10} } it will extract the fields
     * ["a", "b"] to be able to follow the collection and return the value (10).
     */
    fields.push(key.to_string());
    let value = match sub_query {
        Value::Object(map) => map
            .into_iter()
            .find_map(|(key, val)| {
                // if the last element is an object, return that
                // e.g, when setting an object {"a": {"$set": {"b": 30} }
                // which would set {"a": {"b": 30} }
                if key.chars().next() == Some('$') {
                    fields.push(key.to_string());
                    return Some(val.clone());
                }
                return Some(parse_query(val, key, fields));
            })
            .expect("Error while parsing query"),
        Value::Bool(b) => Value::Bool(*b),
        Value::Number(n) => Value::Number(n.clone()),
        Value::String(s) => Value::String(s.to_string()),
        _ => panic!("Not Valid query"),
    };

    return value;
}
