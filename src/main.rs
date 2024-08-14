use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::thread;

use mysql::prelude::*;
use mysql::*;
use regex::Regex;
use signal_hook::consts::SIGINT;
use signal_hook::iterator::Signals;
use tokio::task;
use urlencoding::decode;

const ENV_PATH: &str = ".env";
const ENV_DEFAULT: &str =
    "WIKICRAWL_USER=root\nWIKICRAWL_PASSWORD=root\nWIKICRAWL_HOST=localhost\nWIKICRAWL_PORT=3306\nWIKICRAWL_CONCURRENT_PAGES=200";

#[derive(Debug)]
struct Page {
    id: usize,
    url: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let (url, max_concurrent_pages) = get_url().unwrap();
    let pool = Pool::new(url.as_str()).unwrap();
    let mut connection = pool.get_conn().unwrap();

    let shared_bool = Arc::new(Mutex::new(true));
    let shared_bool_clone = Arc::clone(&shared_bool);
    let mut signals = Signals::new(&[SIGINT])?;
    thread::spawn(move || {
        for sig in signals.forever() {
            if sig != SIGINT {
                continue;
            }

            let mut val = shared_bool.lock().unwrap();
            if *val {
                println!("\nSIGINT received, waiting for the program to stop");
                *val = false;
            } else {
                println!("\nSIGINT received again, forcing the program to stop");
                std::process::exit(0);
            }
        }
    });

    let mut last_query: String;
    while *shared_bool_clone.lock().unwrap() {
        // get greatest page id
        println!("\ngetting greatest page id");
        last_query = "SELECT MAX(id) FROM Pages;".to_string();
        let greatest_id_result: Result<Option<usize>, Error> = connection.query_first(&last_query);
        if greatest_id_result.is_err() {
            eprintln!("\nlast query: {}", last_query);
            return Err(greatest_id_result.unwrap_err());
        }
        let greatest_id = greatest_id_result.unwrap().unwrap_or(0);

        // get unexplored pages
        println!("getting unexplored pages");
        last_query = format!(
            "SELECT id, url FROM Pages WHERE explored = false ORDER BY id ASC LIMIT {};",
            max_concurrent_pages
        );
        let unexplored_result = connection.query_map(&last_query, |(id, url)| Page { id, url });
        if unexplored_result.is_err() {
            eprintln!("\nlast query: {}", last_query);
            return Err(unexplored_result.unwrap_err());
        }
        let unexplored_pages = unexplored_result.unwrap();
        let unexplored_length = unexplored_pages.len();
        if unexplored_length < 1 {
            eprintln!("\nlast query: {}", last_query);
            return Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No unexplored pages found",
            )));
        }

        println!(
            "Exploring pages: {}",
            unexplored_pages
                .iter()
                .map(|page| format!("\"{}\"", page.url))
                .collect::<Vec<String>>()
                .join(", ")
        );

        let pages: Arc<Mutex<Vec<(Page, Option<Vec<String>>)>>> = Arc::new(Mutex::new(Vec::new()));
        let mut children: Vec<task::JoinHandle<()>> = Vec::new();

        println!("spawning threads");

        for page in unexplored_pages {
            let shared_pages = Arc::clone(&pages);
            children.push(task::spawn(async move {
                let explore_result = explore(&page.url).await;
                let mut thread_pages = shared_pages.lock().unwrap();
                if explore_result.is_ok() {
                    thread_pages.push((page, Some(explore_result.unwrap())));
                } else {
                    thread_pages.push((page, None));
                }
                print!(
                    "\rwaiting for threads to finish {}/{} ({}%)            ",
                    thread_pages.len(),
                    max_concurrent_pages,
                    thread_pages.len() * 100 / max_concurrent_pages
                );
                std::io::stdout().flush().unwrap();
            }));
        }

        print!(
            "waiting for threads to finish 0/{} (0%)",
            max_concurrent_pages
        );
        std::io::stdout().flush().unwrap();

        for child in children.into_iter() {
            let child_result = child.await;
            if child_result.is_err() {
                return Err(Error::from(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    child_result.unwrap_err(),
                )));
            }
            child_result.unwrap();
        }

        println!("\rthreads finished");

        let results = pages.lock().unwrap();
        let found_links = results
            .iter()
            .filter(|(_, links)| links.is_some())
            .map(|(_, links)| links.clone().to_owned().unwrap())
            .flatten()
            .collect::<HashSet<String>>();
        let found_pages = found_links
            .into_iter()
            .enumerate()
            .map(|(id, url)| Page {
                id: greatest_id + id + 1,
                url,
            })
            .collect::<Vec<Page>>();

        // mark as bugged if there are
        if results.iter().filter(|(_, links)| links.is_none()).count() > 0 {
            last_query = format!(
                "UPDATE Pages SET bugged = TRUE WHERE id IN ({});",
                results
                    .iter()
                    .filter(|(_, links)| links.is_none())
                    .map(|(page, _)| page.id.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            );
            print!("marking bugged pages ");
            std::io::stdout().flush().unwrap();
            let bugged_result = connection.query_drop(&last_query);
            if bugged_result.is_err() {
                eprintln!("\nlast query: {}", last_query);
                return Err(bugged_result.unwrap_err());
            }
            println!("(marked {} pages)", connection.affected_rows());
        }

        if found_pages.is_empty() {
            println!("No links found");
        } else {
            // insert new pages
            last_query = format!(
                "INSERT IGNORE INTO Pages (id, url) VALUES {};",
                found_pages
                    .iter()
                    .map(|page| format!("({}, \"{}\")", page.id, page.url))
                    .collect::<Vec<String>>()
                    .join(",")
            );
            print!("inserting new pages ");
            std::io::stdout().flush().unwrap();
            let new_pages_result = connection.query_drop(&last_query);
            if new_pages_result.is_err() {
                eprintln!("\nlast query: {}", last_query);
                return Err(new_pages_result.unwrap_err());
            }
            println!("(inserted {} pages)", found_pages.len());

            // transform the results array into an array of relations between pages
            print!("generating relations ");
            std::io::stdout().flush().unwrap();
            let relations_found = results
                .iter()
                .filter(|(_, links)| links.is_some())
                .map(|(page, links)| {
                    links
                        .as_ref()
                        .unwrap()
                        .iter()
                        .filter(|url| found_pages.iter().any(|page| page.url.eq(*url)))
                        .map(|url| {
                            (
                                page,
                                found_pages.iter().find(|page| page.url.eq(url)).unwrap(),
                            )
                        })
                        .collect::<Vec<(&Page, &Page)>>()
                })
                .flatten()
                .collect::<Vec<(&Page, &Page)>>();
            println!("(finished {} relations)", relations_found.len());

            // insert the new relations
            last_query = format!(
                "INSERT IGNORE INTO Links (linker, linked) VALUES {}",
                relations_found
                    .iter()
                    .map(|(linker, linked)| format!("({},{})", linker.id, linked.id))
                    .collect::<Vec<String>>()
                    .join(", ")
            );
            println!("inserting {} relations", relations_found.len());
            let insert_relations = connection.query_drop(&last_query);
            if insert_relations.is_err() {
                eprintln!("\nlast query: {}", last_query);
                return Err(insert_relations.unwrap_err());
            }
        }

        // mark as explored
        last_query = format!(
            "UPDATE Pages SET explored = TRUE WHERE id IN ({});",
            results
                .iter()
                .map(|(page, _)| page.id.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        );
        print!("marking pages as explored ");
        std::io::stdout().flush().unwrap();
        let mark_unexplored_result = connection.query_drop(&last_query);
        if mark_unexplored_result.is_err() {
            eprintln!("\nlast query: {}", last_query);
            return Err(mark_unexplored_result.unwrap_err());
        }
        println!("(marked {} pages)", connection.affected_rows());
    }
    return Ok(());
}

fn get_url() -> Result<(String, usize), Error> {
    let env_read = std::fs::read_to_string(ENV_PATH);
    if env_read.is_err() {
        let env_write = std::fs::write(ENV_PATH, ENV_DEFAULT);
        if env_write.is_err() {
            return Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Couldn't create .env file: {}", env_write.unwrap_err()),
            )));
        }
        return Err(Error::from(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "Error: .env file not found, created a new one with default values:\n{}",
                ENV_DEFAULT
            ),
        )));
    }

    let env_content = env_read.unwrap();
    if env_content.is_empty() {
        return Err(Error::from(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Error: .env file is empty, delete it to see default values or fill it with the following values:\n{}", ENV_DEFAULT),
        )));
    }

    let vars = env_content
        .split("\n")
        .map(|line| line.split_once("=").unwrap_or(("", "")))
        .map(|(str1, str2)| (str1.split_at(10).1.to_string(), str2.to_string()))
        .collect::<HashMap<String, String>>();

    if !vars.contains_key("USER")
        || !vars.contains_key("PASSWORD")
        || !vars.contains_key("HOST")
        || !vars.contains_key("PORT")
    {
        return Err(Error::from(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Error: .env file is missing some values, delete it to see default values or fill it with the following values:\n{}", ENV_DEFAULT),
        )));
    }

    Ok((
        format!(
            "mysql://{}:{}@{}:{}/wikicrawl",
            vars["USER"], vars["PASSWORD"], vars["HOST"], vars["PORT"]
        ),
        vars["CONCURRENT_PAGES"].parse::<usize>().unwrap_or(200),
    ))
}

async fn explore(url: &String) -> Result<Vec<String>, Error> {
    let regex: Regex = Regex::new(r#"['"]/wiki/([a-zA-Z0-9./=_%\-()]*.)['"]"#).unwrap();

    let body = reqwest::get(format!("https://fr.m.wikipedia.org/wiki/{}", url))
        .await
        .unwrap()
        .text()
        .await;

    if body.is_err() {
        return Err(Error::from(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!(
                "Error: Couldn't get text of page {}:\n{}",
                url,
                body.unwrap_err()
            ),
        )));
    }

    let found_links = regex
        .captures_iter(body.unwrap().as_str())
        .map(|captures| {
            let mut first_quote: bool = true;
            decode(captures.get(1).unwrap().as_str())
                .unwrap()
                .into_owned()
                .chars()
                .map(|c| match c {
                    '\\' => String::new(),
                    '"' => {
                        if first_quote {
                            first_quote = false;
                            "«".to_string()
                        } else {
                            first_quote = true;
                            "»".to_string()
                        }
                    }
                    _ => c.to_string(),
                })
                .collect::<String>()
        })
        .collect::<HashSet<String>>()
        .into_iter()
        .collect::<Vec<String>>();

    if found_links.is_empty() {
        return Err(Error::from(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "No links found",
        )));
    }

    Ok(found_links)
}
