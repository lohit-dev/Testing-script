use csv::ReaderBuilder;
use rust_xlsxwriter::{Workbook, XlsxError};
use std::path::{Path, PathBuf};

fn parse_file_arg() -> Result<PathBuf, String> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--file" {
            if let Some(path) = args.next() {
                return Ok(PathBuf::from(path));
            }
            return Err("Missing value for --file".to_string());
        }
    }
    Err("Missing required argument: --file /path/to/file.csv".to_string())
}

fn csv_to_xlsx(input_path: &Path, output_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .from_path(input_path)?;

    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();

    for (row_idx, result) in reader.records().enumerate() {
        let record = result?;
        let row = u32::try_from(row_idx).map_err(|_| "Too many rows for xlsx format")?;

        for (col_idx, value) in record.iter().enumerate() {
            let col = u16::try_from(col_idx).map_err(|_| "Too many columns for xlsx format")?;
            worksheet.write_string(row, col, value)?;
        }
    }

    workbook
        .save(output_path)
        .map_err(|e: XlsxError| -> Box<dyn std::error::Error> { Box::new(e) })?;

    Ok(())
}

fn main() {
    let input_path = match parse_file_arg() {
        Ok(path) => path,
        Err(err) => {
            eprintln!("Error: {}", err);
            eprintln!("Usage: cargo run --bin convert -- --file /path/to/file.csv");
            std::process::exit(1);
        }
    };

    if !input_path.exists() {
        eprintln!("Error: Input file does not exist: {}", input_path.display());
        std::process::exit(1);
    }

    let output_path = input_path.with_extension("xlsx");
    match csv_to_xlsx(&input_path, &output_path) {
        Ok(()) => {
            println!("Created Excel file: {}", output_path.display());
        }
        Err(err) => {
            eprintln!("Conversion failed: {}", err);
            std::process::exit(1);
        }
    }
}
