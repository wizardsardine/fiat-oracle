use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition};

use crate::currency::{Currency, FETCH_ERROR, NO_DATA};
use crate::error::Result;
use crate::paging::{SAMPLES_PER_PAGE, SAMPLE_INTERVAL_SECS};

// One table per currency in a single oracle.redb file. Table names come from
// `Display for Currency` (the same UPPERCASE serde tag used elsewhere).
fn table_def(name: &str) -> TableDefinition<'_, u32, [u32; SAMPLES_PER_PAGE]> {
    TableDefinition::new(name)
}

pub struct Storage {
    db: Database,
}

impl Storage {
    pub fn open(data_dir: &Path, currencies: &[Currency]) -> Result<Self> {
        let path = data_dir.join("oracle.redb");
        let db = Database::create(path)?;
        // Create-if-missing pre-pass: only write txns can create tables, so
        // touch each currency's table once up front. After this, get_page can
        // assume the table exists even if no put_page has run yet.
        let tx = db.begin_write()?;
        {
            for &c in currencies {
                let name = c.to_string();
                let _ = tx.open_table(table_def(&name))?;
            }
        }
        tx.commit()?;
        Ok(Self { db })
    }

    pub fn get_page(
        &self,
        currency: Currency,
        page_id: u32,
    ) -> Result<Option<Box<[u32; SAMPLES_PER_PAGE]>>> {
        let name = currency.to_string();
        let tx = self.db.begin_read()?;
        let table = tx.open_table(table_def(&name))?;
        Ok(table.get(page_id)?.map(|v| Box::new(v.value())))
    }

    pub fn put_page(
        &self,
        currency: Currency,
        page_id: u32,
        page: &[u32; SAMPLES_PER_PAGE],
    ) -> Result<()> {
        let name = currency.to_string();
        let tx = self.db.begin_write()?;
        {
            let mut table = tx.open_table(table_def(&name))?;
            table.insert(page_id, page)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Returns the unix timestamp of the earliest real price sample for `currency`.
    ///
    /// Iterates the B-tree from the lowest page_id and returns on the first slot
    /// that is neither NO_DATA nor FETCH_ERROR. Returns None if the table is empty
    /// or contains only sentinel values.
    pub fn first_timestamp(&self, currency: Currency) -> Result<Option<u64>> {
        const PAGE_SPAN_SECS: u64 = 1u64 << crate::paging::PAGE_SPAN_BITS;

        let name = currency.to_string();
        let tx = self.db.begin_read()?;
        let table = tx.open_table(table_def(&name))?;
        for entry in table.iter()? {
            let (key, value) = entry?;
            let page_id = key.value() as u64;
            let page = value.value();
            for (offset, &sample) in page.iter().enumerate() {
                if sample != NO_DATA && sample != FETCH_ERROR {
                    let ts = page_id * PAGE_SPAN_SECS + offset as u64 * SAMPLE_INTERVAL_SECS;
                    return Ok(Some(ts));
                }
            }
        }
        Ok(None)
    }

    /// Returns the unix timestamp of the latest real price sample for `currency`.
    ///
    /// Iterates the B-tree from the highest page_id and returns on the first slot
    /// that is neither NO_DATA nor FETCH_ERROR. Returns None if the table is empty
    /// or contains only sentinel values.
    pub fn last_timestamp(&self, currency: Currency) -> Result<Option<u64>> {
        const PAGE_SPAN_SECS: u64 = 1u64 << crate::paging::PAGE_SPAN_BITS;

        let name = currency.to_string();
        let tx = self.db.begin_read()?;
        let table = tx.open_table(table_def(&name))?;
        for entry in table.iter()?.rev() {
            let (key, value) = entry?;
            let page_id = key.value() as u64;
            let page = value.value();
            for (offset, &sample) in page.iter().enumerate().rev() {
                if sample != NO_DATA && sample != FETCH_ERROR {
                    let ts = page_id * PAGE_SPAN_SECS + offset as u64 * SAMPLE_INTERVAL_SECS;
                    return Ok(Some(ts));
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn open_creates_file() {
        let dir = tempdir().unwrap();
        Storage::open(dir.path(), &[Currency::Usd]).unwrap();
        assert!(dir.path().join("oracle.redb").exists());
    }

    #[test]
    fn round_trip() {
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path(), &[Currency::Eur]).unwrap();
        let page = [42u32; SAMPLES_PER_PAGE];
        storage.put_page(Currency::Eur, 0, &page).unwrap();
        let back = storage.get_page(Currency::Eur, 0).unwrap().unwrap();
        assert_eq!(*back, page);
    }

    #[test]
    fn get_missing_page_returns_none() {
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path(), &[Currency::Gbp]).unwrap();
        assert!(storage.get_page(Currency::Gbp, 99).unwrap().is_none());
    }

    #[test]
    fn multiple_pages() {
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path(), &[Currency::Gbp]).unwrap();
        let page0 = [1u32; SAMPLES_PER_PAGE];
        let page7 = [7u32; SAMPLES_PER_PAGE];
        storage.put_page(Currency::Gbp, 0, &page0).unwrap();
        storage.put_page(Currency::Gbp, 7, &page7).unwrap();
        assert_eq!(*storage.get_page(Currency::Gbp, 0).unwrap().unwrap(), page0);
        assert_eq!(*storage.get_page(Currency::Gbp, 7).unwrap().unwrap(), page7);
    }

    #[test]
    fn reopen_durability() {
        let dir = tempdir().unwrap();
        {
            let storage = Storage::open(dir.path(), &[Currency::Gbp]).unwrap();
            let page = [123u32; SAMPLES_PER_PAGE];
            storage.put_page(Currency::Gbp, 5, &page).unwrap();
        }
        let storage = Storage::open(dir.path(), &[Currency::Gbp]).unwrap();
        let back = storage.get_page(Currency::Gbp, 5).unwrap().unwrap();
        assert_eq!(*back, [123u32; SAMPLES_PER_PAGE]);
    }

    #[test]
    fn tables_isolated() {
        // Same page_id in different currency tables must not collide.
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path(), &[Currency::Usd, Currency::Eur]).unwrap();
        let usd_page = [11u32; SAMPLES_PER_PAGE];
        let eur_page = [22u32; SAMPLES_PER_PAGE];
        storage.put_page(Currency::Usd, 0, &usd_page).unwrap();
        storage.put_page(Currency::Eur, 0, &eur_page).unwrap();
        assert_eq!(
            *storage.get_page(Currency::Usd, 0).unwrap().unwrap(),
            usd_page
        );
        assert_eq!(
            *storage.get_page(Currency::Eur, 0).unwrap().unwrap(),
            eur_page
        );
    }

    #[test]
    fn last_timestamp_empty_db_returns_none() {
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path(), &[Currency::Usd]).unwrap();
        assert_eq!(storage.last_timestamp(Currency::Usd).unwrap(), None);
    }

    #[test]
    fn last_timestamp_all_sentinels_returns_none() {
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path(), &[Currency::Usd]).unwrap();
        let mut page = [NO_DATA; SAMPLES_PER_PAGE];
        page[1] = FETCH_ERROR;
        storage.put_page(Currency::Usd, 0, &page).unwrap();
        assert_eq!(storage.last_timestamp(Currency::Usd).unwrap(), None);
    }

    #[test]
    fn last_timestamp_finds_last_real_sample() {
        use crate::paging::{PAGE_SPAN_BITS, SAMPLE_INTERVAL_SECS};
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path(), &[Currency::Usd]).unwrap();

        let mut page = [NO_DATA; SAMPLES_PER_PAGE];
        let offset = 5usize;
        page[offset] = 42u32;
        storage.put_page(Currency::Usd, 3, &page).unwrap();

        let expected_ts = 3u64 * (1u64 << PAGE_SPAN_BITS) + offset as u64 * SAMPLE_INTERVAL_SECS;
        assert_eq!(
            storage.last_timestamp(Currency::Usd).unwrap(),
            Some(expected_ts)
        );
    }

    #[test]
    fn last_timestamp_prefers_later_page() {
        use crate::paging::PAGE_SPAN_BITS;
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path(), &[Currency::Usd]).unwrap();

        let mut page1 = [NO_DATA; SAMPLES_PER_PAGE];
        page1[0] = 1u32;
        let mut page5 = [NO_DATA; SAMPLES_PER_PAGE];
        page5[0] = 5u32;
        storage.put_page(Currency::Usd, 1, &page1).unwrap();
        storage.put_page(Currency::Usd, 5, &page5).unwrap();

        let expected_ts = 5u64 * (1u64 << PAGE_SPAN_BITS);
        assert_eq!(
            storage.last_timestamp(Currency::Usd).unwrap(),
            Some(expected_ts)
        );
    }

    #[test]
    fn first_timestamp_empty_db_returns_none() {
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path(), &[Currency::Usd]).unwrap();
        assert_eq!(storage.first_timestamp(Currency::Usd).unwrap(), None);
    }

    #[test]
    fn first_timestamp_all_sentinels_returns_none() {
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path(), &[Currency::Usd]).unwrap();
        // Page filled only with NO_DATA (0) and FETCH_ERROR (u32::MAX) sentinels.
        let mut page = [NO_DATA; SAMPLES_PER_PAGE];
        page[1] = FETCH_ERROR;
        storage.put_page(Currency::Usd, 0, &page).unwrap();
        assert_eq!(storage.first_timestamp(Currency::Usd).unwrap(), None);
    }

    #[test]
    fn first_timestamp_finds_first_real_sample() {
        use crate::paging::{PAGE_SPAN_BITS, SAMPLE_INTERVAL_SECS};
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path(), &[Currency::Usd]).unwrap();

        // Write a real value at page_id=2, offset=3.
        let mut page = [NO_DATA; SAMPLES_PER_PAGE];
        let offset = 3usize;
        page[offset] = 42u32; // any non-sentinel value
        storage.put_page(Currency::Usd, 2, &page).unwrap();

        let expected_ts = 2u64 * (1u64 << PAGE_SPAN_BITS) + offset as u64 * SAMPLE_INTERVAL_SECS;
        assert_eq!(
            storage.first_timestamp(Currency::Usd).unwrap(),
            Some(expected_ts)
        );
    }

    #[test]
    fn first_timestamp_prefers_earlier_page() {
        use crate::paging::PAGE_SPAN_BITS;
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path(), &[Currency::Usd]).unwrap();

        // Write real values in page 5 and page 1; page 1 must win.
        let mut page1 = [NO_DATA; SAMPLES_PER_PAGE];
        page1[0] = 1u32;
        let mut page5 = [NO_DATA; SAMPLES_PER_PAGE];
        page5[0] = 5u32;
        storage.put_page(Currency::Usd, 5, &page5).unwrap();
        storage.put_page(Currency::Usd, 1, &page1).unwrap();

        let expected_ts = 1u64 << PAGE_SPAN_BITS;
        assert_eq!(
            storage.first_timestamp(Currency::Usd).unwrap(),
            Some(expected_ts)
        );
    }
}
