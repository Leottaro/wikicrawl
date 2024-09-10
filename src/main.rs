use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use chrono::Local;
use env_logger::Builder;
use log::{error, info, warn, LevelFilter};
use mysql::prelude::*;
use mysql::*;
use regex::Regex;
use signal_hook::consts::SIGINT;
use signal_hook::iterator::Signals;
use tokio::task;
use urlencoding::decode;

const ENV_PATH: &str = ".env";
const ENV_DEFAULT: &str =
    "WIKICRAWL_USER=root\nWIKICRAWL_PASSWORD=root\nWIKICRAWL_HOST=localhost\nWIKICRAWL_PORT=3306\nWIKICRAWL_CONCURRENT_PAGES=10\nWIKICRAWL_CHUNK_SIZE=200\n";
const WIKIPEDIA_NAMESPACES: [&str; 28] = [
    "média:",
    "spécial:",
    "discussion:",
    "utilisateur:",
    "discussion_utilisateur:",
    "wikipédia:",
    "discussion_wikipédia:",
    "fichier:",
    "discussion_fichier:",
    "mediawiki:",
    "discussion_mediawiki:",
    "modèle:",
    "discussion_modèle:",
    "aide:",
    "discussion_aide:",
    "catégorie:",
    "discussion_catégorie:",
    "portail:",
    "discussion_portail:",
    "projet:",
    "discussion_projet:",
    "référence:",
    "discussion_référence:",
    "timedtext:",
    "timedtext_talk:",
    "module:",
    "discussion_module:",
    "sujet:",
];
const RETRY_COOLDOWN: u64 = 1;

#[derive(Debug, Hash, PartialEq, Eq)]
struct Page {
    id: usize,
    url: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    setup_logs()?;

    let env_result = get_env();
    if env_result.is_err() {
        error_and_log(&format!("{}", env_result.as_ref().unwrap_err()));
        return Err(env_result.unwrap_err());
    }
    let (database_url, max_concurrent_pages, chunk_size) = env_result.unwrap();

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
                println_and_log("SIGINT received, waiting for the program to stop");
                *val = false;
            } else {
                println_and_log("SIGINT received again, forcing the program to stop");
                std::process::exit(0);
            }
        }
    });

    let mut last_query: String;
    while {
        let can_continue = shared_bool_clone.lock().unwrap();
        *can_continue
    } {
        // get unexplored pages
        last_query = format!(
            "SELECT id, url FROM Pages WHERE explored = false AND bugged = false ORDER BY id ASC LIMIT {};",
            max_concurrent_pages
        );
        println_and_log("getting unexplored pages");
        let unexplored_result = connection.query_map(&last_query, |(id, url)| Page { id, url });
        if unexplored_result.is_err() {
            error_and_log(&format!("\nlast query: {}", last_query));
            return Err(unexplored_result.unwrap_err());
        }
        let unexplored_pages = unexplored_result.unwrap();
        let unexplored_length = unexplored_pages.len();
        if unexplored_length < 1 {
            error_and_log(&format!("\nlast query: {}", last_query));
            return Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No unexplored pages found",
            )));
        }
        println_and_log(&format!(
            "Exploring pages: {}",
            unexplored_pages
                .iter()
                .map(|page| format!("\"{}\"", page.url))
                .collect::<Vec<String>>()
                .join(", ")
        ));

        let mut results: Vec<(Page, Vec<String>)> = Vec::new();
        let mut bugged_pages: Vec<Page> = Vec::new();
        let mut children: Vec<task::JoinHandle<(Page, Option<Vec<String>>)>> = Vec::new();
        let shared_explore_regex = Arc::new(Mutex::new(
            Regex::new(r#"(?m)"\/wiki\/([^"\/]+?)(?:#.+?)?"[ >]"#).unwrap(),
        ));

        println_and_log(&format!("spawning threads"));

        unexplored_pages.into_iter().for_each(|page| {
            let thread_explore_regex = Arc::clone(&shared_explore_regex);
            let child = task::spawn(async move {
                let explore_result =
                    explore(&format_url_for_reqwest(&page.url), thread_explore_regex).await;
                print!(
                    "waiting for threads to finish 0/{} (0%)    \r",
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
            "waiting for threads to finish 0/{} (0%)    \r",
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
        println_and_log(&format!(
            "\rthreads finished                                "
        ));

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
            info!("marking bugged pages ");
            std::io::stdout().flush().unwrap();
            let bugged_result = connection.query_drop(&last_query);
            if bugged_result.is_err() {
                error_and_log(&format!("\nlast query: {}", last_query));
                return Err(bugged_result.unwrap_err());
            }
            println_and_log(&format!("({} pages)", connection.affected_rows()));
        }

        let found_links = results
            .iter()
            .map(|(_, links)| links.clone())
            .flatten()
            .collect::<HashSet<String>>();
        println_and_log(&format!("found {} links", found_links.len()));

        let now = Instant::now();
        last_query = format!(
			"SELECT Alias.alias, Pages.id, Pages.url FROM Pages JOIN Alias ON Pages.id = Alias.id WHERE alias IN ({});", 
			found_links
				.iter()
				.map(|link| format!("\"{}\"", format_link_for_mysql(link)))
				.collect::<Vec<String>>()
				.join(", "));
        let found_pages_result = connection
            .query_map(&last_query, |(alias, id, url): (String, usize, String)| {
                (alias, Page { id, url })
            });
        if found_pages_result.is_err() {
            error_and_log(&format!("\nlast query: {}", last_query));
            return Err(found_pages_result.unwrap_err());
        }
        let mut found_pages = found_pages_result
            .unwrap()
            .into_iter()
            .collect::<HashMap<String, Page>>();

        print!(
            "found {} pages ({}ms)      \r",
            found_pages.len(),
            now.elapsed().as_millis()
        );
        std::io::stdout().flush().unwrap();

        let shared_links = Arc::new(Mutex::new(
            found_links
                .into_iter()
                .filter(|link| !found_pages.contains_key(link))
                .collect::<Vec<String>>()
                .into_iter(),
        ));
        let shared_count = Arc::new(Mutex::new(found_pages.len()));
        let shared_now = Arc::new(Mutex::new(Instant::now()));
        let shared_api_regex = Arc::new(Mutex::new(
            Regex::new(r#","title":"(.+)","pageid":([0-9]+),"#).unwrap(),
        ));
        let shared_web_regex = Arc::new(Mutex::new(Regex::new(
			r#"(?m)"wgTitle":"(.*?)",\n?"wgCurRevisionId":[0-9]+,\n?"wgRevisionId":[0-9]+,\n?"wgArticleId":([0-9]+),"#,
		).unwrap()));

        let new_pages_children = (0..chunk_size).into_iter().map(|_| {
            let mut thread_pages: Vec<(String, Page)> = Vec::new();
            let thread_links = Arc::clone(&shared_links);
            let thread_count = Arc::clone(&shared_count);
            let thread_now = Arc::clone(&shared_now);
            let thread_api_regex = Arc::clone(&shared_api_regex);
            let thread_web_regex = Arc::clone(&shared_web_regex);
            task::spawn(async move {
                while let Some(link) = {
                    let mut links = thread_links.lock().unwrap();
                    (*links).next()
                } {
                    let page = extract_link_info_api(
                        format_url_for_reqwest(&link),
                        &thread_api_regex,
                        &thread_web_regex,
                    )
                    .await;
                    let (elapsed, count) = {
                        let now = thread_now.lock().unwrap();
                        let mut count = thread_count.lock().unwrap();
                        *count += 1;
                        (now.elapsed().as_millis(), *count)
                    };
                    print!("found {} pages ({}ms)      \r", count, elapsed);
                    thread_pages.push((link.to_string(), page));
                }
                thread_pages
            })
        });
        let new_pages = new_pages_children
            .into_iter()
            .map(|child| async move { child.await.unwrap() });
        let new_pages = futures::future::join_all(new_pages)
            .await
            .into_iter()
            .flatten()
            .collect::<Vec<(String, Page)>>();
        found_pages.extend(new_pages);
        println_and_log(&format!(
            "found {} pages ({}ms)      ",
            found_pages.len(),
            shared_now.lock().unwrap().elapsed().as_millis()
        ));

        if found_pages.is_empty() {
            println_and_log(&format!("No links found"));
        } else {
            // insert new aliases
            last_query = format!(
                "INSERT IGNORE INTO Alias (alias, id) VALUES {};",
                found_pages
                    .iter()
                    .map(|(alias, page)| format!(
                        "(\"{}\", {})",
                        format_link_for_mysql(alias),
                        page.id
                    ))
                    .collect::<Vec<String>>()
                    .join(",")
            );
            print!("inserting new aliases ");
            info!("inserting new aliases ");
            std::io::stdout().flush().unwrap();
            let new_alias_result = connection.query_drop(&last_query);
            if new_alias_result.is_err() {
                error_and_log(&format!("\nlast query: {}", last_query));
                return Err(new_alias_result.unwrap_err());
            }
            println_and_log(&format!("({} pages)", connection.affected_rows()));

            // insert new pages
            last_query = format!(
                "INSERT IGNORE INTO Pages (id, url) VALUES {};",
                found_pages
                    .iter()
                    .map(|(_, page)| format!(
                        "({}, \"{}\")",
                        page.id,
                        format_link_for_mysql(&page.url)
                    ))
                    .collect::<Vec<String>>()
                    .join(",")
            );
            print!("inserting new pages ");
            info!("inserting new pages ");
            std::io::stdout().flush().unwrap();
            let new_pages_result = connection.query_drop(&last_query);
            if new_pages_result.is_err() {
                error_and_log(&format!("\nlast query: {}", last_query));
                return Err(new_pages_result.unwrap_err());
            }
            println_and_log(&format!("({} pages)", connection.affected_rows()));

            // transform the results array into an array of relations between pages
            print!("generating relations ");
            info!("generating relations ");
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
            println_and_log(&format!("({} relations)", relations_found.len()));

            // insert the new relations
            last_query = format!(
                "INSERT IGNORE INTO Links (linker, linked) VALUES {};",
                relations_found
                    .iter()
                    .map(|(linker, linked)| format!("({},{})", linker.id, linked.id))
                    .collect::<Vec<String>>()
                    .join(", ")
            );
            print!("inserting the relations ");
            info!("inserting the relations ");
            std::io::stdout().flush().unwrap();
            let insert_relations = connection.query_drop(&last_query);
            if insert_relations.is_err() {
                error_and_log(&format!("\nlast query: {}", last_query));
                return Err(insert_relations.unwrap_err());
            }
            println_and_log(&format!("({} relations)", connection.affected_rows()));
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
        info!("marking pages as explored ");
        std::io::stdout().flush().unwrap();
        let mark_unexplored_result = connection.query_drop(&last_query);
        if mark_unexplored_result.is_err() {
            error_and_log(&format!("\nlast query: {}", last_query));
            return Err(mark_unexplored_result.unwrap_err());
        }
        println_and_log(&format!("({} pages)", connection.affected_rows()));

        // status info
        println_and_log(&format!(
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
        ));
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
        .filter_map(|line| line.split_once("="))
        .map(|(str1, str2)| (str1.to_string().split_off(10), str2.to_string()))
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

async fn explore(url: &String, regex: Arc<Mutex<Regex>>) -> Result<Vec<String>, Error> {
    let request = format!("https://fr.m.wikipedia.org/wiki/{}", url);

    let body = reqwest::get(request.clone())
        .await
        .unwrap_or_else(|error| {
            println!("{}", url);
            panic!("{}", error);
        })
        .text()
        .await
        .unwrap();

    let found_links = {
        let owned_regex = regex.lock().unwrap();
        (*owned_regex)
            .captures_iter(body.as_str())
            .map(|captures| {
                decode(captures.get(1).unwrap().as_str())
                    .unwrap()
                    .into_owned()
                    .to_ascii_lowercase()
            })
            .collect::<HashSet<String>>()
    };

    if found_links.is_empty() {
        error_and_log(&format!("No links found in: {}", request));
        std::process::exit(0);
    }

    let filtered_links = found_links
        .into_iter()
        .filter(|link| {
            !WIKIPEDIA_NAMESPACES
                .iter()
                .any(|namespace| link.starts_with(namespace))
        })
        .collect::<Vec<String>>();

    if filtered_links.is_empty() {
        error_and_log(&format!("No links found in: {}", request));
        std::process::exit(0);
    }

    Ok(filtered_links)
}

async fn extract_link_info_api(
    url: String,
    api_regex: &Arc<Mutex<Regex>>,
    web_regex: &Arc<Mutex<Regex>>,
) -> Page {
    let request = format!(
		"https://fr.m.wikipedia.org/w/api.php?action=query&format=json&list=search&utf8=1&formatversion=2&srnamespace=0&srlimit=1&srsearch={}", 
		url
	);

    if url.len() > 98 {
        return extract_link_info_web(url, web_regex).await;
    }

    loop {
        let body = reqwest::get(&request)
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
            .replace("\n", "");

        if !body.starts_with('{') {
            std::thread::sleep(Duration::from_secs(RETRY_COOLDOWN));
            continue;
        }

        if !body.starts_with("{\"batchcomplete\":true,") || body.ends_with("\"search\":[]}}") {
            // à envoyer au web
            warn_and_log(&format!("API can't find #\"{}\"#", request));
            return extract_link_info_web(url, web_regex).await;
        }

        let captures = {
            let owned_regex = api_regex.lock().unwrap();
            (*owned_regex).captures(body.as_str())
        };
        match captures {
            Some(capture) => {
                return Page {
                    url: capture.get(1).unwrap().as_str().replace("\\\"", "\""),
                    id: capture.get(2).unwrap().as_str().parse::<usize>().unwrap(),
                };
            }
            None => {
                error_and_log(&format!("error: no match in body: {}\n\n\n", body));
                std::process::exit(0);
            }
        }
    }
}

async fn extract_link_info_web(url: String, regex: &Arc<Mutex<Regex>>) -> Page {
    let request = format!("https://fr.m.wikipedia.org/wiki/Spécial:Recherche/{}", url);
    loop {
        let body = reqwest::get(&request)
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
            .replace("\n", "");

        let captures = {
            let owned_regex = regex.lock().unwrap();
            (*owned_regex).captures(body.as_str())
        };
        match captures {
            Some(capture) => {
                return Page {
                    url: capture.get(1).unwrap().as_str().to_string(),
                    id: capture.get(2).unwrap().as_str().parse::<usize>().unwrap(),
                };
            }
            None => {
                error_and_log(&format!(
                    "error: no match in body for url {}: \n{}\n\n\n",
                    url, body
                ));
                std::process::exit(0);
            }
        }
    }
}

fn setup_logs() -> Result<(), Error> {
    std::fs::DirBuilder::new().recursive(true).create("logs")?;
    let mut log_name = Local::now().format("%Y-%m-%d").to_string();
    let existing_logs = std::fs::read_dir("logs")?;
    let this_day_logs = existing_logs
        .into_iter()
        .filter_map(|file| match file {
            Ok(entry) => match entry.file_name().into_string() {
                Ok(name) => {
                    if name.starts_with(&log_name) {
                        Some(name)
                    } else {
                        None
                    }
                }
                Err(_) => None,
            },
            Err(_) => None,
        })
        .count();
    if this_day_logs > 0 {
        log_name.push_str(&format!("({})", this_day_logs + 1));
    }

    let target =
        Box::new(File::create(format!("logs/{}.log", log_name)).expect("Can't create file"));

    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{}-[{}]: {}",
                Local::now().format("%Y-%m-%d_%H:%M:%S%.3f"),
                record.level(),
                record.args()
            )
        })
        .target(env_logger::Target::Pipe(target))
        .filter(None, LevelFilter::Info)
        .init();

    info!("Starting the program");
    Ok(())
}

fn format_link_for_mysql(link: &String) -> String {
    link.chars()
        .map(|char| match char {
            '\\' => "\\\\".to_string(),
            '"' => "\"\"".to_string(),
            _ => char.to_string(),
        })
        .collect()
}

fn format_url_for_reqwest(url: &String) -> String {
    url.chars()
        .map(|char| match char {
            '&' => "%26".to_string(),
            _ => char.to_string(),
        })
        .collect()
}

fn println_and_log(message: &str) {
    println!("{}", message);
    info!("{}", message);
}

fn warn_and_log(message: &str) {
    eprintln!("{}", message);
    warn!("{}", message);
}

fn error_and_log(message: &str) {
    eprintln!("{}", message);
    error!("{}", message);
}
