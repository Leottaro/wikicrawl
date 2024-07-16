use std::collections::{HashMap, HashSet};

use mysql::prelude::*;
use mysql::*;
use regex::Regex;
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

    let url = format!(
        "mysql://{}:{}@{}:{}/wikicrawl",
        vars["USER"], vars["PASSWORD"], vars["HOST"], vars["PORT"]
    );
    println!("Connecting to {}", url);

    let pool: &Pool = &Pool::new(url.as_str()).unwrap();
    let connection: &mut PooledConn = &mut pool.get_conn().unwrap();

    let mut offset: usize = 0;

    loop {
        let exploring_pages = get_n_unexplored(connection, offset + 1);
        if exploring_pages.is_err() {
            offset += 1;
            continue;
        }
        let pages = exploring_pages.unwrap();
        let page = pages.get(offset).unwrap();

        let explore_result = explore(connection, page).await;
        if explore_result.is_err() {
            println!(
                "Error happened during page ({}, \"{}\") explore: {}",
                page.id,
                page.url,
                explore_result.unwrap_err()
            );
            offset += 1;
        }

        let total_pages = connection
            .query_first("SELECT COUNT(*) FROM Pages;")
            .unwrap_or(Some(-1))
            .unwrap_or(-1);

        let unexplored_pages = connection
            .query_first("SELECT COUNT(*) FROM Unexplored;")
            .unwrap_or(Some(-1))
            .unwrap_or(-1);

        println!(
            "Explored pages: {}/{} ({}%)",
            total_pages - unexplored_pages,
            total_pages,
            (total_pages - unexplored_pages) as f32 / (total_pages as f32) * 100.0
        );
    }
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

fn get_n_unexplored(connection: &mut PooledConn, n: usize) -> Result<Vec<Page>, Error> {
    connection
        .query_map(
            format!("SELECT Pages.id, Pages.url FROM Unexplored JOIN Pages ON Pages.id = Unexplored.id ORDER BY Unexplored.id ASC LIMIT {};", n),
            |(id, url)| Page { id, url },
        )
}

async fn explore(connection: &mut PooledConn, page: &Page) -> Result<(), Error> {
    println!("\nExploring page ({}, \"{}\")", page.id, page.url);
    let regex: Regex = Regex::new(r#"['"]/wiki/([a-zA-Z0-9./=_%\-()]*.)['"]"#).unwrap();

    let body = reqwest::get(format!("https://fr.wikipedia.org/wiki/{}", page.url))
        .await
        .map_err(|err| println!("Error: Couldn't reach page {}: {}", page.url, err))
        .unwrap()
        .text()
        .await
        .map_err(|err| println!("Error: Couldn't get text of page {}: {}", page.url, err))
        .unwrap();

    let found_links: Vec<String> = regex
        .captures_iter(&body)
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
        eprintln!(
            "an Error occured while creating new pages of page ({}, \"{}\"): {}",
            page.id,
            page.url,
            found_pages.as_ref().unwrap_err(),
        );
        return Err(found_pages.unwrap_err());
    }

    let link_error = new_links(connection, &page, &found_pages.unwrap());
    if link_error.is_err() {
        eprintln!(
            "an Error occured while creating new links of page ({}, \"{}\"): {}",
            page.id,
            page.url,
            link_error.as_ref().unwrap_err()
        );
        return Err(link_error.unwrap_err());
    }

    let delete_error = connection.exec_drop(
        "DELETE FROM Unexplored WHERE id = :id;",
        params! {"id" => page.id},
    );
    if delete_error.is_err() {
        eprintln!(
            "Error deleting unexplored page (id: {}, url: \"{}\"): {}",
            page.id,
            page.url,
            delete_error.as_ref().unwrap_err()
        );
        return Err(delete_error.unwrap_err());
    }

    Ok(())
}

fn new_pages(connection: &mut PooledConn, urls: &Vec<String>) -> Result<Vec<Page>, Error> {
    println!("Creating new pages");
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
    println!("Creating new links");
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
