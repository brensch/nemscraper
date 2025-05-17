use anyhow::Result;
use csv::ReaderBuilder;
use std::{collections::BTreeMap, fs::File, path::Path};
use zip::ZipArchive;

#[derive(Debug)]
pub struct RawTable {
    /// column names, from the “I” row
    pub headers: Vec<String>,
    /// each “D” row, as a Vec of Strings (one per field)
    pub rows: Vec<Vec<String>>,
}

/// Open `zip_path`, find all `.csv` entries, and for each
/// - on “I” rows: start a new schema table (keyed by schema name)
/// - on “D” rows: push the data row into the current schema

pub fn load_aemo_zip<P: AsRef<Path>>(zip_path: P) -> Result<BTreeMap<String, RawTable>> {
    let file = File::open(zip_path)?;
    let mut archive = ZipArchive::new(file)?;
    let mut tables: BTreeMap<String, RawTable> = BTreeMap::new();
    let mut current: Option<String> = None;

    for i in 0..archive.len() {
        let f = archive.by_index(i)?;
        if f.is_file() && f.name().ends_with(".csv") {
            let mut rdr = ReaderBuilder::new()
                .has_headers(false)
                .flexible(true)
                .from_reader(f);

            for record in rdr.records() {
                let record = record?;
                match record.get(0) {
                    Some("I") => {
                        // 1) Grab the schema name
                        let schema = record.get(2).unwrap().to_string();
                        // 2) Collect the column names
                        let cols: Vec<String> = record.iter().skip(4).map(str::to_string).collect();

                        // 3) Insert or fetch a RawTable, then overwrite headers via a clone
                        let table = tables.entry(schema.clone()).or_insert(RawTable {
                            headers: Vec::new(),
                            rows: Vec::new(),
                        });
                        table.headers = cols.clone(); // <-- clone so `cols` itself isn’t moved

                        current = Some(schema);
                    }
                    Some("D") => {
                        if let Some(ref schema) = current {
                            let row: Vec<String> =
                                record.iter().skip(4).map(str::to_string).collect();
                            tables.get_mut(schema).unwrap().rows.push(row);
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(tables)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::io::{Cursor, Write};
    use tempfile::NamedTempFile;
    use zip::write::FileOptions;
    use zip::CompressionMethod;

    #[test]
    fn test_load_aemo_zip_example() -> Result<()> {
        // build the exact example CSV you provided
        let content = r#"C,SETP.WORLD,FPP_DCF,AEMO,PUBLIC,2024/12/14,18:02:37,0000000443338900,FPP,0000000443338900
I,FPP,FORECAST_DEFAULT_CF,1,FPP_UNITID,CONSTRAINTID,EFFECTIVE_START_DATETIME,EFFECTIVE_END_DATETIME,VERSIONNO,BIDTYPE,REGIONID,DEFAULT_CONTRIBUTION_FACTOR,DCF_REASON_FLAG,DCF_ABS_NEGATIVE_PERF_TOTAL,SETTLEMENTS_UNITID
D,FPP,FORECAST_DEFAULT_CF,1,ADPBA1,F_I+GFT_TG_R5,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,SA1,-0.00011729,0,520.67692,ADPBA1
D,FPP,FORECAST_DEFAULT_CF,1,ADPBA1,F_I+LREG_0210,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,LOWERREG,SA1,-0.00035807,0,415.19675,ADPBA1
D,FPP,FORECAST_DEFAULT_CF,1,ADPBA1,F_I+NIL_APD_TL_L5,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,LOWERREG,SA1,-0.00035807,0,415.19675,ADPBA1
D,FPP,FORECAST_DEFAULT_CF,1,ADPBA1,F_I+NIL_MG_R5,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,SA1,-0.00011729,0,520.67692,ADPBA1
D,FPP,FORECAST_DEFAULT_CF,1,ADPBA1,F_I+RREG_0220,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,SA1,-0.00011729,0,520.67692,ADPBA1
D,FPP,FORECAST_DEFAULT_CF,1,ADPBA1,F_I+TTS_TG_R5,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,SA1,-0.00011729,0,520.67692,ADPBA1
D,FPP,FORECAST_DEFAULT_CF,1,ADPBA1,F_MAIN++APD_TL_L5,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,LOWERREG,SA1,-0.00038632,0,384.84063,ADPBA1
D,FPP,FORECAST_DEFAULT_CF,1,ADPBA1,F_MAIN++GFT_TG_R5,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,SA1,-0.00012270,0,497.72470,ADPBA1
D,FPP,FORECAST_DEFAULT_CF,1,ADPBA1,F_MAIN++LREG_0210,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,LOWERREG,SA1,-0.00038632,0,384.84063,ADPBA1
D,FPP,FORECAST_DEFAULT_CF,1,ADPBA1,F_MAIN++NIL_MG_R5,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,SA1,-0.00012270,0,497.72470,ADPBA1
D,FPP,FORECAST_DEFAULT_CF,1,ADPBA1,F_MAIN++RREG_0220,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,SA1,-0.00012270,0,497.72470,ADPBA1
D,FPP,FORECAST_DEFAULT_CF,1,ADPBA1,F_MAIN+APD_TL_L5,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,LOWERREG,SA1,-0.00038632,0,384.84063,ADPBA1
D,FPP,FORECAST_DEFAULT_CF,1,ADPBA1,F_MAIN+LREG_0210,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,LOWERREG,SA1,-0.00038632,0,384.84063,ADPBA1
D,FPP,FORECAST_DEFAULT_CF,1,YWPS4,F_MAIN+LREG_0210,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,LOWERREG,VIC1,0.00000000,0,384.84063,YWPS4
D,FPP,FORECAST_DEFAULT_CF,1,YWPS4,F_MAIN+NIL_MG_R5,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,VIC1,0.00000000,0,497.72470,YWPS4
D,FPP,FORECAST_DEFAULT_CF,1,YWPS4,F_MAIN+RREG_0220,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,VIC1,0.00000000,0,497.72470,YWPS4
D,FPP,FORECAST_DEFAULT_CF,1,YWPS4,F_MAIN+TTS_TG_R5,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,VIC1,0.00000000,0,497.72470,YWPS4
D,FPP,FORECAST_DEFAULT_CF,1,YWPS4,F_TASCAP_LREG_0210,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,LOWERREG,VIC1,0.00000000,0,384.84063,YWPS4
D,FPP,FORECAST_DEFAULT_CF,1,YWPS4,F_TASCAP_RREG_0220,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,VIC1,0.00000000,0,497.72470,YWPS4
I,FPP,FORECAST_RESIDUAL_DCF,1,CONSTRAINTID,EFFECTIVE_START_DATETIME,EFFECTIVE_END_DATETIME,VERSIONNO,BIDTYPE,RESIDUAL_DCF,RESIDUAL_DCF_REASON_FLAG,DCF_ABS_NEGATIVE_PERF_TOTAL
D,FPP,FORECAST_RESIDUAL_DCF,1,F_I+GFT_TG_R5,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,-0.22384015,0,520.67692
D,FPP,FORECAST_RESIDUAL_DCF,1,F_I+LREG_0210,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,LOWERREG,-0.19742411,0,415.19675
D,FPP,FORECAST_RESIDUAL_DCF,1,F_I+NIL_APD_TL_L5,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,LOWERREG,-0.19742411,0,415.19675
D,FPP,FORECAST_RESIDUAL_DCF,1,F_I+NIL_MG_R5,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,-0.22384015,0,520.67692
D,FPP,FORECAST_RESIDUAL_DCF,1,F_I+RREG_0220,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,-0.22384015,0,520.67692
D,FPP,FORECAST_RESIDUAL_DCF,1,F_I+TTS_TG_R5,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,-0.22384015,0,520.67692
D,FPP,FORECAST_RESIDUAL_DCF,1,F_MAIN++APD_TL_L5,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,LOWERREG,-0.19072919,0,384.84063
D,FPP,FORECAST_RESIDUAL_DCF,1,F_MAIN++GFT_TG_R5,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,-0.21921217,0,497.72470
D,FPP,FORECAST_RESIDUAL_DCF,1,F_MAIN++LREG_0210,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,LOWERREG,-0.19072919,0,384.84063
D,FPP,FORECAST_RESIDUAL_DCF,1,F_MAIN++NIL_MG_R5,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,-0.21921217,0,497.72470
D,FPP,FORECAST_RESIDUAL_DCF,1,F_MAIN++RREG_0220,"2024/12/22 00:05:00","2024/12/29 00:00:00",2,RAISEREG,-0.21921217,0,497.72470"#;

        // write ZIP
        let mut buf = Vec::new();
        {
            let mut zip = zip::ZipWriter::new(Cursor::new(&mut buf));
            let options: FileOptions<'_, ()> =
                FileOptions::default().compression_method(CompressionMethod::Stored);
            zip.start_file("aemo.csv", options)?;
            zip.write_all(content.as_bytes())?;
            zip.finish()?;
        }

        let mut tmp = NamedTempFile::new()?;
        tmp.write_all(&buf)?;

        // run loader
        let tables = load_aemo_zip(tmp.path())?;

        // we should get two schemas
        assert_eq!(tables.len(), 2);
        let default = &tables["FORECAST_DEFAULT_CF"];
        let residual = &tables["FORECAST_RESIDUAL_DCF"];

        // correct headers?
        assert_eq!(
            default.headers,
            vec![
                "FPP_UNITID",
                "CONSTRAINTID",
                "EFFECTIVE_START_DATETIME",
                "EFFECTIVE_END_DATETIME",
                "VERSIONNO",
                "BIDTYPE",
                "REGIONID",
                "DEFAULT_CONTRIBUTION_FACTOR",
                "DCF_REASON_FLAG",
                "DCF_ABS_NEGATIVE_PERF_TOTAL",
                "SETTLEMENTS_UNITID"
            ]
        );
        assert_eq!(default.rows.len(), 19);

        assert_eq!(
            residual.headers,
            vec![
                "CONSTRAINTID",
                "EFFECTIVE_START_DATETIME",
                "EFFECTIVE_END_DATETIME",
                "VERSIONNO",
                "BIDTYPE",
                "RESIDUAL_DCF",
                "RESIDUAL_DCF_REASON_FLAG",
                "DCF_ABS_NEGATIVE_PERF_TOTAL"
            ]
        );
        assert_eq!(residual.rows.len(), 11);

        Ok(())
    }
}
