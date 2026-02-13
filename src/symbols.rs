use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

pub struct SymbolMapper {
    symbol_to_id: HashMap<String, u64>,
}

impl SymbolMapper {
    pub fn load_from_tsv(path: &str) -> Result<Self, String> {
        let file = File::open(path)
            .map_err(|e| format!("Failed to open symbols.tsv: {}", e))?;

        let reader = BufReader::new(file);
        let mut symbol_to_id = HashMap::new();

        for (line_num, line) in reader.lines().enumerate() {
            let line = line
                .map_err(|e| format!("Error reading line {}: {}", line_num + 1, e))?;

            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() != 2 {
                return Err(format!("Invalid format at line {}: expected 2 columns, got {}",
                    line_num + 1, parts.len()));
            }

            let symbol_id = parts[0].trim().parse::<u64>()
                .map_err(|e| format!("Invalid symbol_id at line {}: {}", line_num + 1, e))?;

            let symbol = parts[1].trim().to_uppercase();

            symbol_to_id.insert(symbol, symbol_id);
        }

        Ok(SymbolMapper { symbol_to_id })
    }

    pub fn get_id(&self, symbol: &str) -> Option<u64> {
        self.symbol_to_id.get(&symbol.to_uppercase()).copied()
    }

    pub fn len(&self) -> usize {
        self.symbol_to_id.len()
    }
}

pub fn load_subscribe_list(path: &str) -> Result<Vec<String>, String> {
    let file = File::open(path)
        .map_err(|e| format!("Failed to open subscribe file: {}", e))?;

    let reader = BufReader::new(file);
    let mut symbols = Vec::new();

    for line in reader.lines() {
        let line = line
            .map_err(|e| format!("Error reading subscribe file: {}", e))?;

        let symbol = line.trim();
        if !symbol.is_empty() {
            symbols.push(symbol.to_uppercase());
        }
    }

    Ok(symbols)
}

pub fn validate_symbols(
    subscribe_symbols: &[String],
    mapper: &SymbolMapper,
) -> Result<HashMap<String, u64>, String> {
    let mut result = HashMap::new();

    for symbol in subscribe_symbols {
        match mapper.get_id(symbol) {
            Some(id) => {
                result.insert(symbol.clone(), id);
            }
            None => {
                return Err(format!("Symbol {} not found in symbols.tsv", symbol));
            }
        }
    }

    Ok(result)
}
