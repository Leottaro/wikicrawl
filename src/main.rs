use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use mysql::prelude::*;
use mysql::*;
use regex::{Match, Regex};
use signal_hook::consts::SIGINT;
use signal_hook::iterator::Signals;
use tokio::task;
use urlencoding::decode;

const ENV_PATH: &str = ".env";
const ENV_DEFAULT: &str =
    "WIKICRAWL_USER=root\nWIKICRAWL_PASSWORD=root\nWIKICRAWL_HOST=localhost\nWIKICRAWL_PORT=3306\nWIKICRAWL_CONCURRENT_PAGES=10\nWIKICRAWL_CHUNK_SIZE=200\n";
const IMPOSSIBLE_PAGES: [&str; 10] = [
    "fichier:",
    "projet:",
    "portail:",
    "aide:",
    "spécial:",
    "référence:",
    "modèle:",
    "catégorie:",
    "discussion:",
    "wikipédia:",
];
const RETRY_COOLDOWN: u64 = 1;

#[derive(Debug, Hash, PartialEq, Eq)]
struct Page {
    id: usize,
    url: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let (database_url, max_concurrent_pages, chunk_size) = get_env().unwrap();
    let pool = Pool::new(database_url.clone().as_str()).unwrap();
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
        // get unexplored pages
        last_query = format!(
            "SELECT id, url FROM Pages WHERE explored = false AND bugged = false ORDER BY id ASC LIMIT {};",
            max_concurrent_pages
        );
        println!("getting unexplored pages");
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

        let mut results: Vec<(Page, Vec<String>)> = Vec::new();
        let mut bugged_pages: Vec<Page> = Vec::new();
        let mut children: Vec<task::JoinHandle<(Page, Option<Vec<String>>)>> = Vec::new();

        println!("spawning threads");

        unexplored_pages.into_iter().for_each(|page| {
            let child = task::spawn(async move {
                let explore_result = explore(&page.url).await;
                print!(
                    "waiting for threads to finish 0/{} (0%)\t\r",
                    max_concurrent_pages
                );
                std::io::stdout().flush().unwrap();
                match explore_result {
                    Ok(links) => (page, Some(links)),
                    Err(_) => (page, None),
                }
            });
            children.push(child);
        });

        print!(
            "waiting for threads to finish 0/{} (0%)\t\r",
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
            let (page, links) = child_result.unwrap();
            match links {
                Some(links) => results.push((page, links)),
                None => bugged_pages.push(page),
            }
        }
        println!("\rthreads finished                                ");

        // mark as bugged if there are
        if bugged_pages.len() > 0 {
            last_query = format!(
                "UPDATE Pages SET bugged = TRUE WHERE id IN ({});",
                bugged_pages
                    .iter()
                    .map(|page| page.id.to_string())
                    .collect::<Vec<String>>()
                    .join(",")
            );
            print!("marking bugged pages ");
            std::io::stdout().flush().unwrap();
            let bugged_result = connection.query_drop(&last_query);
            if bugged_result.is_err() {
                eprintln!("\nlast query: {}", last_query);
                return Err(bugged_result.unwrap_err());
            }
            println!("({} pages)", connection.affected_rows());
        }

        let found_links = results
            .iter()
            .map(|(_, links)| links)
            .flatten()
            .collect::<HashSet<&String>>();
        println!("found {} links", found_links.len());

        let now = Instant::now();
        last_query = format!(
			"SELECT Alias.alias, Pages.id, Pages.url FROM Pages JOIN Alias ON Pages.id = Alias.id WHERE alias IN ({});", 
			found_links
				.iter()
				.map(|link| format!("\"{}\"", link))
				.collect::<Vec<String>>()
				.join(", "));
        let mut found_pages = connection
            .query_map(last_query, |(alias, id, url): (String, usize, String)| {
                (alias, Page { id, url })
            })
            .unwrap()
            .into_iter()
            .collect::<HashMap<String, Page>>();
        print!(
            "found {} pages ({}ms)      \r",
            found_pages.len(),
            now.elapsed().as_millis()
        );

        let mut total_elapsed: u128 = 0;
        for chunk in found_links
            .into_iter()
            .filter(|link| !found_pages.contains_key(link.to_owned()))
            .collect::<Vec<&String>>()
            .chunks(chunk_size)
        {
            let now = Instant::now();
            let new_pages_children = chunk.into_iter().map(|link| {
                let owned_link = link.to_string();
                task::spawn(
                    async move { (owned_link.clone(), extract_link_info_api(owned_link).await) },
                )
            });
            let new_pages = new_pages_children
                .into_iter()
                .map(|child| async move { child.await.unwrap() });
            found_pages.extend(futures::future::join_all(new_pages).await);

            let elapsed = now.elapsed().as_millis();
            total_elapsed += elapsed;
            print!("found {} pages ({}ms)      \r", found_pages.len(), elapsed);
            std::io::stdout().flush().unwrap();
        }
        println!(
            "found {} pages ({}ms)     ",
            found_pages.len(),
            total_elapsed
        );

        if found_pages.is_empty() {
            println!("No links found");
        } else {
            // insert new aliases
            last_query = format!(
                "INSERT IGNORE INTO Alias (alias, id) VALUES {};",
                found_pages
                    .iter()
                    .map(|(alias, page)| format!(
                        "(\"{}\", {})",
                        alias.replace("\"", "\"\""),
                        page.id
                    ))
                    .collect::<Vec<String>>()
                    .join(",")
            );
            print!("inserting new aliases");
            std::io::stdout().flush().unwrap();
            let new_alias_result = connection.query_drop(&last_query);
            if new_alias_result.is_err() {
                eprintln!("\nlast query: {}", last_query);
                return Err(new_alias_result.unwrap_err());
            }
            println!("({} pages)", found_pages.len());

            // insert new pages
            last_query = format!(
                "INSERT IGNORE INTO Pages (id, url) VALUES {};",
                found_pages
                    .iter()
                    .map(|(_, page)| format!(
                        "({}, \"{}\")",
                        page.id,
                        page.url.replace("\"", "\"\"")
                    ))
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
            println!("({} pages)", found_pages.len());

            // transform the results array into an array of relations between pages
            print!("generating relations ");
            std::io::stdout().flush().unwrap();

            let relations_found = results
                .iter()
                .map(|(page, links)| {
                    links
                        .iter()
                        .filter_map(|url| {
                            let linked = found_pages.get(url);
                            linked.map(|link| (page, link))
                        })
                        .collect::<HashSet<(&Page, &Page)>>()
                })
                .flatten()
                .collect::<HashSet<(&Page, &Page)>>();
            println!("({} relations)", relations_found.len());

            // insert the new relations
            last_query = format!(
                "INSERT IGNORE INTO Links (linker, linked) VALUES {};",
                relations_found
                    .iter()
                    .map(|(linker, linked)| format!("({},{})", linker.id, linked.id))
                    .collect::<Vec<String>>()
                    .join(", ")
            );
            println!("inserting the relations");
            let insert_relations = connection.query_drop(&last_query);
            if insert_relations.is_err() {
                eprintln!("\nlast query: {}", last_query);
                return Err(insert_relations.unwrap_err());
            }
        }

        // mark as explored
        last_query = format!(
            "UPDATE Pages SET explored = TRUE WHERE id BETWEEN {} AND {};",
            results
                .iter()
                .min_by(|x, y| x.0.id.cmp(&y.0.id))
                .unwrap()
                .0
                .id,
            results
                .iter()
                .max_by(|x, y| x.0.id.cmp(&y.0.id))
                .unwrap()
                .0
                .id
        );
        print!("marking pages as explored ");
        std::io::stdout().flush().unwrap();
        let mark_unexplored_result = connection.query_drop(&last_query);
        if mark_unexplored_result.is_err() {
            eprintln!("\nlast query: {}", last_query);
            return Err(mark_unexplored_result.unwrap_err());
        }
        println!("({} pages)", connection.affected_rows());

        // status info
        println!(
            "\nexplored {} pages (with {} bugged) \nfound {} pages \nlisted {} links\n",
            connection
                .query_first("SELECT COUNT(*) FROM Pages WHERE explored = TRUE;")
                .unwrap()
                .unwrap_or(-1),
            connection
                .query_first("SELECT COUNT(*) FROM Pages WHERE bugged = TRUE;")
                .unwrap()
                .unwrap_or(-1),
            connection
                .query_first("SELECT COUNT(*) FROM Pages;")
                .unwrap()
                .unwrap_or(-1),
            connection
                .query_first("SELECT COUNT(*) FROM Links;")
                .unwrap()
                .unwrap_or(-1)
        );
    }

    return Ok(());
}

fn get_env() -> Result<(String, usize, usize), Error> {
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
        || !vars.contains_key("CONCURRENT_PAGES")
        || !vars.contains_key("CHUNK_SIZE")
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
        vars["CONCURRENT_PAGES"].parse::<usize>().unwrap_or(100),
        vars["CHUNK_SIZE"].parse::<usize>().unwrap_or(200),
    ))
}

async fn explore(url: &String) -> Result<Vec<String>, Error> {
    let regex = Regex::new(r#"(?m)"\/wiki\/([^"\/]+)""#).unwrap();

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
                    '?' => "%3F".to_string(),
                    _ => c.to_lowercase().to_string(),
                })
                .collect::<String>()
        })
        .collect::<HashSet<String>>()
        .into_iter()
        .collect::<Vec<String>>();

    let filtered_links = found_links
        .into_iter()
        .filter(|link| {
            !IMPOSSIBLE_PAGES
                .iter()
                .any(|impossible| link.to_ascii_lowercase().starts_with(impossible))
        })
        .collect::<Vec<String>>();

    if filtered_links.is_empty() {
        return Err(Error::from(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "No links found",
        )));
    }

    Ok(filtered_links)
}

async fn extract_link_info_api(url: String) -> Page {
    let regex = Regex::new(r#"(?mU),"title":"(.+)","pageid":(.+),"size""#).unwrap();
    let mut loop_count = 0;

    while loop_count < 3 {
        let body = reqwest::get(format!(
            "https://fr.m.wikipedia.org/w/api.php?action=query&format=json&list=search&utf8=1&formatversion=2&srnamespace=0&srlimit=1&srsearch={}",
            url
        ))
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
        .replace("\n", "");

        let captures = regex.captures(body.as_str());
        match captures {
            Some(capture) => {
                return Page {
                    url: capture.get(1).unwrap().as_str().to_string(),
                    id: capture.get(2).unwrap().as_str().parse::<usize>().unwrap(),
                };
            }
            None => {
                if !body.starts_with("{\"batchcomplete\":true,\"continue\"") {
                    loop_count += 1;
                    thread::sleep(Duration::from_secs(RETRY_COOLDOWN));
                    continue;
                } else {
                    println!("API couldn't find {}: \n{}", url, body);
                    loop_count = 3;
                }
            }
        }
    }
    extract_link_info_web(url).await
}

async fn extract_link_info_web(url: String) -> Page {
    let regex = Regex::new(r#"(?m)"?(?:(?:wgArticleId)|(?:wgPageName))"?:\n?"?(.*?)"?,"#).unwrap();

    loop {
        let body = reqwest::get(format!(
            "https://fr.m.wikipedia.org/wiki/Spécial:Recherche/{}",
            url
        ))
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
        .replace("\n", "");

        let captures = regex
            .captures_iter(body.as_str())
            .map(|capture| capture.get(1).unwrap())
            .collect::<Vec<Match>>();
        let found_url = captures.get(0).map(|content| content.as_str());
        let found_id = captures.get(1).map(|content| content.as_str());
        if found_url.is_some() && found_id.is_some() {
            return Page {
                url: found_url.unwrap().to_string(),
                id: found_id.unwrap().parse::<usize>().unwrap(),
            };
        }
        thread::sleep(Duration::from_secs(RETRY_COOLDOWN));
    }
}
