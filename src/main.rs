use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use mysql::prelude::*;
use mysql::*;
use regex::Regex;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::task;
use urlencoding::decode;

const ENV_PATH: &str = ".env";
const ENV_DEFAULT: &str =
    "WIKICRAWL_USER=root\nWIKICRAWL_PASSWORD=root\nWIKICRAWL_HOST=localhost\nWIKICRAWL_PORT=3306";

#[derive(Debug)]
struct Page {
    id: i32,
    url: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let env = read_env();
    if env.is_err() {
        return Err(env.unwrap_err());
    }

    let vars = env.unwrap();
    if !vars.contains_key("USER")
        || !vars.contains_key("PASSWORD")
        || !vars.contains_key("HOST")
        || !vars.contains_key("PORT")
    {
        eprintln!("Error: .env file is missing some values, delete it to see default values or fill it with the following values:\n{}", ENV_DEFAULT);
        return Err(Error::from(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            ".env file is missing some values, see error message",
        )));
    }

    let url = Arc::new(RwLock::new(format!(
        "mysql://{}:{}@{}:{}/wikicrawl",
        vars["USER"], vars["PASSWORD"], vars["HOST"], vars["PORT"]
    )));

    let main_pool: &Pool = &Pool::new(url.read().expect("unable to read URL").as_str()).unwrap();
    let main_connection: &mut PooledConn = &mut main_pool.get_conn().unwrap();

    let (tx, mut rx) = mpsc::channel::<i32>(255);
    let num_children = 16;
    let mut child_count = 0;

    for child_id in 0..num_children {
        println!("Init child {}", child_id);
        let err = spawn_child(main_connection, tx.clone(), url.clone(), child_id);
        if err.is_err() {
            eprintln!(
                "Error failed to spawn child {}: {}",
                child_id,
                err.unwrap_err()
            );
        } else {
            child_count += 1;
        }
    }

    while let Some(child_id) = rx.recv().await {
        // a child has terminated
        println!("child {} has terminated", child_id);
        child_count -= 1;

        let err = spawn_child(main_connection, tx.clone(), url.clone(), child_id);
        if err.is_err() {
            eprintln!(
                "Error failed to spawn child {}: {}",
                child_id,
                err.unwrap_err()
            );
        } else {
            child_count += 1;
        }

        if child_count >= num_children {
            continue;
        }
        println!("Init child {}", child_count);
        let err2 = spawn_child(main_connection, tx.clone(), url.clone(), child_count);
        if err2.is_err() {
            eprintln!(
                "Error failed to spawn child {}: {}",
                child_count,
                err2.unwrap_err()
            );
        } else {
            child_count += 1;
        }
    }

    Ok(())
}

fn read_env() -> Result<HashMap<String, String>, Error> {
    let env_read = std::fs::read_to_string(ENV_PATH);
    if env_read.is_err() {
        let env_write = std::fs::write(ENV_PATH, ENV_DEFAULT);
        if env_write.is_err() {
            eprintln!(
                "Error: Couldn't create .env file: {}",
                env_write.unwrap_err()
            );
            return Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Couldn't create .env file",
            )));
        }
        eprintln!(
            "Error: .env file not found, created a new one with default values:\n{}",
            ENV_DEFAULT
        );
        return Err(Error::from(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            ".env file not found, see error message",
        )));
    }

    let env_content = env_read.unwrap();
    if env_content.is_empty() {
        eprintln!("Error: .env file is empty, delete it to see default values or fill it with the following values:\n{}", ENV_DEFAULT);
        return Err(Error::from(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            ".env file is empty, see error message",
        )));
    }

    Ok(env_content
        .split("\n")
        .map(|line| line.split_once("=").unwrap_or(("", "")))
        .map(|(str1, str2)| (str1.split_at(10).1.to_string(), str2.to_string()))
        .collect::<HashMap<String, String>>())
}

fn spawn_child(
    main_connection: &mut PooledConn,
    shared_tx: Sender<i32>,
    shared_url: Arc<RwLock<String>>,
    child_id: i32,
) -> Result<(), Error> {
    let unexplored_result = get_unexplored(main_connection);
    if unexplored_result.is_err() {
        return Err(unexplored_result.unwrap_err());
    }
    let page = unexplored_result.unwrap();

    let mark_bugged_result = mark_bugged(main_connection, &page);
    if mark_bugged_result.is_err() {
        return Err(mark_bugged_result.unwrap_err());
    }

    task::spawn(async move {
        let url = shared_url.read().expect("Unable to read url 1").clone();
        let pool: &Pool = &Pool::new(url.as_str()).unwrap();
        let connection = &mut pool.get_conn().unwrap();
        let result = explore(connection, &page).await;
        if result.is_err() {
            // println!("Error: {}", result.unwrap_err());
        }
        shared_tx.send(child_id).await.unwrap();
    });

    Ok(())
}

fn get_unexplored(connection: &mut PooledConn) -> Result<Page, Error> {
    connection
        .query_first("SELECT Pages.id, Pages.url FROM Unexplored JOIN Pages ON Unexplored.id = Pages.id WHERE Unexplored.bugged = FALSE ORDER BY Pages.id ASC;")
        .map_or_else(
            |err| Err(err),
            |row: Option<Row>| {
                if row.is_none() {
                    Err(Error::from(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "Error: No unexplored pages found",
                    )))
                } else {
                    let temp = row.unwrap();
                    Ok(Page {
                        id: temp.get(0).unwrap(),
                        url: temp.get(1).unwrap(),
                    })
                }
            },
        )
}

fn mark_bugged(connection: &mut PooledConn, page: &Page) -> Result<(), Error> {
    connection.exec_drop(
        "UPDATE Unexplored SET bugged = TRUE WHERE id = :id;",
        params! {"id" => page.id},
    )
}

async fn explore(connection: &mut PooledConn, page: &Page) -> Result<(), Error> {
    // println!("\nExploring page ({}, \"{}\")", page.id, page.url);
    let regex: Regex = Regex::new(r#"['"]/wiki/([a-zA-Z0-9./=_%\-()]*.)['"]"#).unwrap();

    let body = reqwest::get(format!("https://fr.wikipedia.org/wiki/{}", page.url))
        .await
        // .map_err(|err| println!("Error: Couldn't reach page {}: {}", page.url, err))
        .unwrap()
        .text()
        .await;
    if body.is_err() {
        return Err(Error::from(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!(
                "Error: Couldn't get text of page {}: {}",
                page.url,
                body.unwrap_err()
            ),
        )));
    }

    let found_links: Vec<String> = regex
        .captures_iter(body.unwrap().as_str())
        .map(|captures| {
            decode(captures.get(1).unwrap().as_str())
                .unwrap()
                .into_owned()
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    if found_links.is_empty() {
        return Err(Error::from(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "No links found",
        )));
    }

    let found_pages: Result<Vec<Page>, Error> = new_pages(connection, &found_links);
    if found_pages.is_err() {
        // eprintln!(
        //     "an Error occured while creating new pages of page ({}, \"{}\"): {}",
        //     page.id,
        //     page.url,
        //     found_pages.as_ref().unwrap_err(),
        // );
        return Err(found_pages.unwrap_err());
    }

    let link_error = new_links(connection, &page, &found_pages.unwrap());
    if link_error.is_err() {
        // eprintln!(
        //     "an Error occured while creating new links of page ({}, \"{}\"): {}",
        //     page.id,
        //     page.url,
        //     link_error.as_ref().unwrap_err()
        // );
        return Err(link_error.unwrap_err());
    }

    let delete_error = connection.exec_drop(
        "DELETE FROM Unexplored WHERE id = :id;",
        params! {"id" => page.id},
    );
    if delete_error.is_err() {
        // eprintln!(
        //     "Error deleting unexplored page (id: {}, url: \"{}\"): {}",
        //     page.id,
        //     page.url,
        //     delete_error.as_ref().unwrap_err()
        // );
        return Err(delete_error.unwrap_err());
    }

    Ok(())
}

fn new_pages(connection: &mut PooledConn, urls: &Vec<String>) -> Result<Vec<Page>, Error> {
    // println!("Creating new pages");
    let insert_query: String = format!(
        "INSERT IGNORE INTO Pages(url) VALUES (\"{}\");",
        urls.join("\"), (\"")
    );

    let select_query: String = format!(
        "SELECT * FROM Pages WHERE url in (\"{}\");",
        urls.join("\", \"")
    );

    let caca: Result<Vec<Page>, Error> = connection.exec_drop(insert_query, ()).map(|_| vec![]);
    if caca.is_err() {
        return caca;
    }
    connection.query_map(select_query, |(id, url)| Page { id, url })
}

fn new_links(
    connection: &mut PooledConn,
    current_page: &Page,
    linked_pages: &Vec<Page>,
) -> Result<(), Error> {
    // println!("Creating new links");
    let inserted_values: Vec<String> = linked_pages
        .into_iter()
        .map(|page| format!("({}, \"{}\")", current_page.id, page.id))
        .collect();

    let query = format!(
        "INSERT IGNORE INTO Links VALUES {};",
        inserted_values.join(",")
    );

    connection.exec_drop(query, ())
}
