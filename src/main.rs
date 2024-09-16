use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::ops::{AddAssign, SubAssign};
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
use tokio::task::JoinHandle;
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

#[derive(Debug)]
struct Page {
    id: usize,
    title: String,
}
impl PartialEq for Page {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Page {}

impl Hash for Page {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Display for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ \"id\": {}, \"title\": \"{}\" }}",
            self.id,
            self.title.replace("\"", "\\\"")
        )
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    setup_logs()?;
    println_and_log("Starting the program");

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

    let mut total_explored: usize = connection
        .query_first("SELECT COUNT(*) FROM Pages WHERE explored = TRUE;")
        .unwrap_or(Some(0))
        .unwrap_or(0);
    let mut total_bugged: usize = connection
        .query_first("SELECT COUNT(*) FROM Pages WHERE bugged = TRUE;")
        .unwrap_or(Some(0))
        .unwrap_or(0);
    let mut total_pages: usize = connection
        .query_first("SELECT COUNT(*) FROM Pages;")
        .unwrap_or(Some(0))
        .unwrap_or(0);
    let mut total_links: usize = connection
        .query_first("SELECT COUNT(*) FROM Links;")
        .unwrap_or(Some(0))
        .unwrap_or(0);

    println_and_log(&format!(
        "\nexplored {} pages (with {} bugged) \nfound {} pages \nlisted {} links\n",
        total_explored, total_bugged, total_pages, total_links
    ));

    let mut last_query: String;
    while {
        let can_continue = shared_bool_clone.lock().unwrap();
        *can_continue
    } {
        // get unexplored pages
        last_query = format!(
            "SELECT id, title FROM Pages WHERE explored = false AND bugged = false ORDER BY id ASC LIMIT {};",
            max_concurrent_pages
        );
        println_and_log("getting unexplored pages");
        let unexplored_result = connection.query_map(&last_query, |(id, title)| Page { id, title });
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
            "Exploring pages: [{}]",
            unexplored_pages
                .iter()
                .map(|page| page.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        ));

        // delete potential links from an old run
        println_and_log("deleting potential links from an old run");
        last_query = format!(
            "DELETE FROM Links WHERE linker IN ({});",
            unexplored_pages
                .iter()
                .map(|page| page.id.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        );
        connection.query_drop(&last_query).unwrap();

        let mut results: Vec<(Page, Vec<String>)> = Vec::new();
        let mut bugged_pages: Vec<Page> = Vec::new();
        let mut children: Vec<JoinHandle<(Page, Option<Vec<String>>)>> = Vec::new();
        let now = Instant::now();
        let shared_explore_regex = Arc::new(Mutex::new(
            Regex::new(r#"(?m)"\/wiki\/([^"\/]+?)(?:#.+?)?"[ >]"#).unwrap(),
        ));
        let shared_explored_count = Arc::new(Mutex::new(0 as usize));

        println_and_log(&format!("exploring pages"));

        unexplored_pages.into_iter().for_each(|page| {
            let thread_explore_regex = Arc::clone(&shared_explore_regex);
            let thread_explored_count = Arc::clone(&shared_explored_count);
            let child = tokio::spawn(async move {
                let explore_result = explore(&page, thread_explore_regex).await;
                let count = {
                    let mut tmp = thread_explored_count.lock().unwrap();
                    (*tmp).add_assign(1);
                    *tmp
                };
                print!(
                    "explored {}/{} pages ({}%)      \r",
                    count,
                    max_concurrent_pages,
                    100 * count / max_concurrent_pages
                );
                std::io::stdout().flush().unwrap();
                match explore_result {
                    Ok(links) => {
                        if links.is_empty() {
                            (page, None)
                        } else {
                            (page, Some(links))
                        }
                    }
                    Err(_) => (page, None),
                }
            });
            children.push(child);
        });

        print!("explored 0/0 pages (0%)      \r");
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
            "\rexplored {} pages in {} ms",
            unexplored_length,
            now.elapsed().as_millis()
        ));

        // mark as bugged if there are
        if !bugged_pages.is_empty() {
            last_query = format!(
                "UPDATE Pages SET bugged = TRUE WHERE id IN ({});",
                bugged_pages
                    .iter()
                    .map(|page| page.id.to_string())
                    .collect::<Vec<String>>()
                    .join(",")
            );
            println_and_log("marking bugged pages");
            let bugged_result = connection.query_drop(&last_query);
            if bugged_result.is_err() {
                error_and_log(&format!("\nlast query: {}", last_query));
                return Err(bugged_result.unwrap_err());
            }
            println_and_log(&format!("marked {} bugged pages", bugged_pages.len()));
            total_bugged += bugged_pages.len();
        }

        let found_links = results
            .iter()
            .map(|(_, links)| links.clone())
            .flatten()
            .collect::<HashSet<String>>();
        println_and_log(&format!("found {} links", found_links.len()));

        if found_links.len() > 0 {
            let now = Instant::now();
            last_query = format!(
			"SELECT Alias.alias, Pages.id, Pages.title FROM Pages JOIN Alias ON Pages.id = Alias.id WHERE alias IN ({});", 
			found_links
				.iter()
				.map(|link| format!("\"{}\"", format_link_for_mysql(link)))
				.collect::<Vec<String>>()
				.join(", "));
            let old_pages_result = connection.query_map(
                &last_query,
                |(alias, id, title): (String, usize, String)| (alias, Page { id, title }),
            );
            if old_pages_result.is_err() {
                error_and_log(&format!("\nlast query: {}", last_query));
                return Err(old_pages_result.unwrap_err());
            }
            let mut old_pages = old_pages_result
                .unwrap()
                .into_iter()
                .collect::<HashMap<String, Page>>();

            println_and_log(&format!(
                "found {} old pages ({}ms)",
                old_pages.len(),
                now.elapsed().as_millis()
            ));

            let new_links = found_links
                .into_iter()
                .filter(|link| !old_pages.contains_key(link))
                .collect::<Vec<String>>();
            let shared_count = Arc::new(Mutex::new(new_links.len()));
            let shared_links = Arc::new(Mutex::new(new_links.into_iter()));
            let shared_now = Arc::new(Mutex::new(Instant::now()));
            let shared_api_regex = Arc::new(Mutex::new(
                Regex::new(r#","title":"(.+)","pageid":([0-9]+),"#).unwrap(),
            ));
            let shared_web_regex = Arc::new(Mutex::new(
				Regex::new(r#"(?m)"wgTitle":"(.*?)",\n?"wgCurRevisionId":[0-9]+,\n?"wgRevisionId":[0-9]+,\n?"wgArticleId":([0-9]+),"#,).unwrap()
			));

            let new_pages_children = (0..chunk_size).into_iter().map(|_| {
                let mut thread_pages: Vec<(String, Page)> = Vec::new();
                let thread_links = Arc::clone(&shared_links);
                let thread_count = Arc::clone(&shared_count);
                let thread_now = Arc::clone(&shared_now);
                let thread_api_regex = Arc::clone(&shared_api_regex);
                let thread_web_regex = Arc::clone(&shared_web_regex);
                tokio::spawn(async move {
                    while let Some(link) = {
                        let mut links = thread_links.lock().unwrap();
                        (*links).next()
                    } {
                        let page =
                            extract_link_info_api(&link, &thread_api_regex, &thread_web_regex)
                                .await;
                        let (elapsed, count) = {
                            let now = thread_now.lock().unwrap();
                            let mut count = thread_count.lock().unwrap();
                            (*count).sub_assign(1);
                            (now.elapsed().as_millis(), *count)
                        };
                        print!("{} pages left to find ({}ms)         \r", count, elapsed);
                        thread_pages.push((link.to_string(), page));
                    }
                    thread_pages
                })
            });
            let future_new_pages = new_pages_children
                .into_iter()
                .map(|child| async move { child.await.unwrap() });
            let found_pages = futures::future::join_all(future_new_pages)
                .await
                .into_iter()
                .flatten()
                .collect::<Vec<(String, Page)>>();

            println_and_log(&format!(
                "found {} pages ({}ms)               ",
                found_pages.len(),
                shared_now.lock().unwrap().elapsed().as_millis()
            ));

            // split found_pages into new_pages and found_again_pages using the connection
            last_query = format!(
                "SELECT id FROM Pages WHERE id IN ({});",
                found_pages
                    .iter()
                    .map(|(_, page)| page.id.to_string())
                    .collect::<Vec<String>>()
                    .join(",")
            );
            let found_again_result = connection.query_map(&last_query, |id: usize| id);
            if found_again_result.is_err() {
                error_and_log(&format!("\nlast query: {}", last_query));
                return Err(found_again_result.unwrap_err());
            }
            let found_again_pages_ids = found_again_result.unwrap();

            let (found_again_pages, new_pages): (HashMap<String, Page>, HashMap<String, Page>) =
                found_pages
                    .into_iter()
                    .filter(|(_, page)| page.id != 0)
                    .partition(|(_, page)| found_again_pages_ids.contains(&page.id));

            old_pages.extend(found_again_pages.into_iter());

            let unique_new_pages = new_pages
                .iter()
                .map(|(_, page)| page)
                .collect::<HashSet<&Page>>();
            let added_pages = unique_new_pages.len();

            println_and_log(&format!(
                "found {} new pages \nfound again {} old pages",
                unique_new_pages.len(),
                old_pages.len()
            ));

            // insert new pages
            if added_pages > 0 {
                total_pages += added_pages;
                last_query = format!(
                    "INSERT INTO Pages (id, title) VALUES {};",
                    unique_new_pages
                        .into_iter()
                        .map(|page| format!(
                            "({}, \"{}\")",
                            page.id,
                            format_link_for_mysql(&page.title)
                        ))
                        .collect::<Vec<String>>()
                        .join(",")
                );
                println_and_log("inserting new pages");
                let new_pages_result = connection.query_drop(&last_query);
                if new_pages_result.is_err() {
                    error_and_log(&format!("\nlast query: {}", last_query));
                    return Err(new_pages_result.unwrap_err());
                }
                println_and_log(&format!("inserted {} new pages", added_pages));
            }

            // insert aliases of new Pages
            if new_pages.len() > 0 {
                last_query = format!(
                    "INSERT INTO Alias (alias, id) VALUES {};",
                    new_pages
                        .iter()
                        .map(|(alias, page)| format!(
                            "(\"{}\",{})",
                            format_link_for_mysql(alias),
                            page.id
                        ))
                        .collect::<Vec<String>>()
                        .join(",")
                );
                println_and_log("inserting aliases of found pages");
                let new_alias_result = connection.query_drop(&last_query);
                if new_alias_result.is_err() {
                    error_and_log(&format!("\nlast query: {}", last_query));
                    return Err(new_alias_result.unwrap_err());
                }
                println_and_log(&format!("inserted {} aliases", new_pages.len()));
            }

            // transform the results array into an array of relations between pages
            println_and_log("generating relations ");
            let relations_found = results
                .iter()
                .map(|(page, links)| {
                    links
                        .iter()
                        .filter_map(|link| {
                            let linked = old_pages.get(link).or(new_pages.get(link));
                            linked.map(|link| (page, link))
                        })
                        .collect::<HashSet<(&Page, &Page)>>()
                })
                .flatten()
                .collect::<HashSet<(&Page, &Page)>>();
            println_and_log(&format!("generated {} relations", relations_found.len()));

            // insert the new relations
            last_query = format!(
                "INSERT INTO Links (linker, linked) VALUES {};",
                relations_found
                    .iter()
                    .map(|(linker, linked)| format!("({},{})", linker.id, linked.id))
                    .collect::<Vec<String>>()
                    .join(", ")
            );
            println_and_log("inserting the relations ");
            let insert_relations = connection.query_drop(&last_query);
            if insert_relations.is_err() {
                error_and_log(&format!("\nlast query: {}", last_query));
                return Err(insert_relations.unwrap_err());
            }
            println_and_log(&format!("inserted {} relations", relations_found.len()));
            total_links += relations_found.len();
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
        println_and_log("marking pages as explored ");
        let mark_unexplored_result = connection.query_drop(&last_query);
        if mark_unexplored_result.is_err() {
            error_and_log(&format!("\nlast query: {}", last_query));
            return Err(mark_unexplored_result.unwrap_err());
        }
        println_and_log(&format!("explored {} pages", unexplored_length));
        total_explored += unexplored_length;

        println_and_log(&format!(
            "\nexplored {} pages (with {} bugged) \nfound {} pages \nlisted {} links\n",
            total_explored, total_bugged, total_pages, total_links
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

async fn explore(page: &Page, regex: Arc<Mutex<Regex>>) -> Result<Vec<String>, Error> {
    let request = format!("https://fr.m.wikipedia.org/?curid={}", page.id);

    loop {
        let body = reqwest::get(request.clone())
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        if body.contains("<title>Wikimedia Error</title>") {
            thread::sleep(Duration::from_secs(RETRY_COOLDOWN));
            continue;
        }

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

        let filtered_links = found_links
            .into_iter()
            .filter(|link| {
                !WIKIPEDIA_NAMESPACES
                    .iter()
                    .any(|namespace| link.starts_with(namespace))
            })
            .collect::<Vec<String>>();

        if filtered_links.is_empty() {
            warn_and_log(&format!(
                "No links found in Page {{ id: {}, title: \"{}\" }}",
                page.id, page.title
            ));
        }

        return Ok(filtered_links);
    }
}

async fn extract_link_info_api(
    url: &String,
    api_regex: &Arc<Mutex<Regex>>,
    web_regex: &Arc<Mutex<Regex>>,
) -> Page {
    let formatted_url = format_url_for_api_reqwest(&url);
    let request = format!(
		"https://fr.m.wikipedia.org/w/api.php?action=query&format=json&list=search&utf8=1&formatversion=2&srnamespace=0&srlimit=1&srsearch={}", 
		formatted_url
	);

    if formatted_url.len() > 98 {
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
                    title: capture.get(1).unwrap().as_str().replace("\\\"", "\""),
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

async fn extract_link_info_web(url: &String, regex: &Arc<Mutex<Regex>>) -> Page {
    let request = format!(
        "https://fr.m.wikipedia.org/wiki/Spécial:Recherche/{}",
        format_url_for_reqwest(url)
    );
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
                    title: capture.get(1).unwrap().as_str().to_string(),
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

fn format_url_for_api_reqwest(url: &String) -> String {
    url.chars()
        .map(|char| match char {
            '&' => "%26".to_string(),
            '*' => "\\*".to_string(),
            _ => char.to_string(),
        })
        .collect()
}

fn format_url_for_reqwest(url: &String) -> String {
    url.chars()
        .map(|char| match char {
            '?' => "%3F".to_string(),
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
