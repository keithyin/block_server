use turso;

use crate::file::check_file_existence;
pub struct ServingInfoDb {
    _db: turso::Database,
    conn: turso::Connection,
}

impl ServingInfoDb {
    pub async fn new(db_path: &str) -> anyhow::Result<Self> {
        let db = turso::Builder::new_local(db_path).build().await?;
        let conn = db.connect()?;
        Ok(ServingInfoDb { _db: db, conn })
    }

    /// tag:
    ///     0 : new arrived, not read yet
    ///     1 : reading
    ///     2 : ready to delete
    pub async fn create_table(&self) -> anyhow::Result<()> {
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS serving_files (fpath TEXT, tag INTEGER)",
                (),
            )
            .await?;
        Ok(())
    }

    pub async fn insert_new_file(&self, fpath: &str) -> anyhow::Result<()> {
        if !check_file_existence(fpath).await {
            tracing::warn!("check_file_existence failed. fpath:{}", fpath);
            return Ok(());
        }
        self.conn
            .execute(
                "INSERT INTO serving_files (fpath, tag) VALUES (?1, ?2)",
                (fpath, 0),
            )
            .await?;
        Ok(())
    }

    pub async fn get_new_arrived_files(&self) -> anyhow::Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT fpath FROM serving_files WHERE tag = 0")
            .await?;
        let mut rows = stmt.query(()).await?;

        let mut fpaths = Vec::new();
        while let Some(row) = rows.next().await? {
            let fpath = row
                .get_value(0)?
                .as_text()
                .ok_or(anyhow::anyhow!("Failed to get fpath"))?
                .to_string();

            if check_file_existence(&fpath).await {
                fpaths.push(fpath);
            } else {
                tracing::warn!("check_file_existence failed. fpath: {}", fpath);
            }
        }
        Ok(fpaths)
    }

    pub async fn get_not_deleted_files(&self) -> anyhow::Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT fpath FROM serving_files WHERE tag != 2")
            .await?;
        let mut rows = stmt.query(()).await?;

        let mut fpaths = Vec::new();
        while let Some(row) = rows.next().await? {
            let fpath = row
                .get_value(0)?
                .as_text()
                .ok_or(anyhow::anyhow!("Failed to get fpath"))?
                .to_string();

            if check_file_existence(&fpath).await {
                fpaths.push(fpath);
            } else {
                tracing::warn!("check_file_existence failed. fpath: {}", fpath);
            }
        }
        Ok(fpaths)
    }

    pub async fn get_files_for_deletion(&self) -> anyhow::Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT fpath FROM serving_files WHERE tag = 2")
            .await?;
        let mut rows = stmt.query(()).await?;

        let mut fpaths = Vec::new();
        while let Some(row) = rows.next().await? {
            let fpath = row
                .get_value(0)?
                .as_text()
                .ok_or(anyhow::anyhow!("Failed to get fpath"))?
                .to_string();
            fpaths.push(fpath);
        }
        Ok(fpaths)
    }

    pub async fn update_file_tag_for_reading(&self, fpath: &str) -> anyhow::Result<()> {
        self.update_file_tag(fpath, 1).await?;
        Ok(())
    }

    pub async fn update_file_tag_for_deletion(&self, fpath: &str) -> anyhow::Result<()> {
        self.update_file_tag(fpath, 2).await?;
        Ok(())
    }

    pub async fn remove_file_record(&self, fpath: &str) -> anyhow::Result<()> {
        self.conn
            .execute("DELETE FROM serving_files WHERE fpath = ?1", (fpath,))
            .await?;
        Ok(())
    }

    pub async fn print_all_records(&self) -> anyhow::Result<()> {
        let mut stmt = self
            .conn
            .prepare("SELECT fpath, tag FROM serving_files")
            .await?;
        let mut rows = stmt.query(()).await?;

        println!("Current serving_files records:");
        while let Some(row) = rows.next().await? {
            let fpath = row
                .get_value(0)?
                .as_text()
                .ok_or(anyhow::anyhow!("Failed to get fpath"))?
                .to_string();
            let tag = *row
                .get_value(1)?
                .as_integer()
                .ok_or(anyhow::anyhow!("Failed to get tag"))?;
            println!("fpath: {}, tag: {}", fpath, tag);
        }
        Ok(())
    }

    // what happened if the file path does not exist?
    async fn update_file_tag(&self, fpath: &str, tag: i32) -> anyhow::Result<()> {
        self.conn
            .execute(
                "UPDATE serving_files SET tag = ?1 WHERE fpath = ?2",
                (tag, fpath),
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::db::ServingInfoDb;
    #[tokio::test]
    async fn test_turso() {
        use turso::Builder;

        let db = Builder::new_local("sqlite.db").build().await.unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE IF NOT EXISTS users (email TEXT)", ())
            .await
            .unwrap();
        conn.execute("INSERT INTO users (email) VALUES ('alice@example.org')", ())
            .await
            .unwrap();

        let mut stmt = conn
            .prepare("SELECT * FROM users WHERE email = ?1")
            .await
            .unwrap();
        let mut rows = stmt.query(["alice@example.org"]).await.unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let value = row.get_value(0).unwrap();
        println!("Row: {:?}", value);
    }
    #[tokio::test]
    async fn test_serving_file_database() {
        let db = ServingInfoDb::new("serving_info.db").await.unwrap();
        db.create_table().await.unwrap();
        db.insert_new_file("/path/to/file1").await.unwrap();
        db.insert_new_file("/path/to/file2").await.unwrap();
        db.print_all_records().await.unwrap();
        println!("-------------------");
        println!(
            "New arrived files:{:?}",
            db.get_new_arrived_files().await.unwrap()
        );

        db.update_file_tag_for_reading("/path/to/file1")
            .await
            .unwrap();
        db.print_all_records().await.unwrap();
        println!("-------------------");
        db.update_file_tag_for_deletion("/path/to/file2")
            .await
            .unwrap();
        db.print_all_records().await.unwrap();

        println!("-------------------");

        println!(
            "New arrived files:{:?}",
            db.get_new_arrived_files().await.unwrap()
        );

        db.remove_file_record("/path/to/file2").await.unwrap();
        db.print_all_records().await.unwrap();
        println!("-------------------");
    }
}
