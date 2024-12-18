use lib::*;

use chrono::Local;
use log::{error, info, warn, LevelFilter};
use log4rs::append::console::{ConsoleAppender, Target};
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::Config;
use mysql::{prelude::*, PooledConn};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::io::Write;
use std::ops::{AddAssign, SubAssign};
use std::sync::{Arc, Mutex};
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration, Instant};
use urlencoding::decode;

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
const MAX_SAME_ERROR: usize = 3;

struct TotalInfo {
    explored: usize,
    bugged: usize,
    pages: usize,
    links: usize,
}

struct Link<'a> {
    linker: usize,
    linked: usize,
    display: &'a String,
}

impl<'a> PartialEq for Link<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.linker == other.linker && self.linked == other.linked
    }
}
impl<'a> Eq for Link<'a> {}

impl<'a> std::hash::Hash for Link<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.linker.hash(state);
        self.linked.hash(state);
    }
}

pub async fn setup_wikicrawl(
    connection: &mut PooledConn,
    max_exploring_pages: usize,
    max_new_pages: usize,
) -> () {
    println!("setting up logs");
    setup_logs().unwrap();
    info!("Starting wikicrawl");

    info!("creating SIGINT thread");
    let sigint_cancel = Arc::new(Mutex::new(false));
    let sigint_cancel_clone = Arc::clone(&sigint_cancel);
    ctrlc::set_handler(move || {
        let mut cancel = sigint_cancel_clone.lock().unwrap();
        match *cancel {
            false => {
                info!("SIGINT received, waiting for the program to stop");
                *cancel = true;
            }
            true => {
                info!("SIGINT received, forcing the program to stop");
                std::process::exit(0);
            }
        }
    })
    .unwrap();

    let mut total_info = TotalInfo {
        explored: 0,
        bugged: 0,
        pages: 0,
        links: 0,
    };
    info!("querying total explored pages");
    total_info.explored = connection
        .query_first("SELECT COUNT(*) FROM Pages WHERE explored = TRUE;")
        .unwrap_or(Some(0))
        .unwrap_or(0);
    info!("querying total bugged pages");
    total_info.bugged = connection
        .query_first("SELECT COUNT(*) FROM Pages WHERE bugged = TRUE;")
        .unwrap_or(Some(0))
        .unwrap_or(0);
    info!("querying total pages");
    total_info.pages = connection
        .query_first("SELECT COUNT(*) FROM Pages;")
        .unwrap_or(Some(0))
        .unwrap_or(0);
    info!("querying total links");
    total_info.links = connection
        .query_first("SELECT COUNT(*) FROM Links;")
        .unwrap_or(Some(0))
        .unwrap_or(0);

    let error_regex = Regex::new(r"(?m)ERROR ([0-9]+) ").unwrap();
    let mut error_count: HashMap<usize, usize> = HashMap::new();
    loop {
        let mut last_query: String = String::new();
        let mut exploring_pages: Vec<Page> = Vec::new();
        *sigint_cancel.lock().unwrap() = false;
        let result = wikicrawl(
            &mut last_query,
            &mut exploring_pages,
            &mut total_info,
            connection,
            &sigint_cancel,
            max_exploring_pages,
            max_new_pages,
        )
        .await;
        *sigint_cancel.lock().unwrap() = true;

        if result.is_err() {
            if !last_query.is_empty() {
                error!("WIKICRAWL CRASHED WITH LAST QUERY BEING");
                error!("{}", last_query);
            } else {
                error!("WIKICRAWL CRASHED");
            }
            let error = result.unwrap_err().to_string();
            error!("{}", error);

            last_query = format!(
                "UPDATE Pages SET bugged = TRUE WHERE id IN ({});",
                exploring_pages
                    .into_iter()
                    .map(|page| page.id.to_string())
                    .collect::<Vec<String>>()
                    .join(",")
            );
            error!("");
            error!("Marking all unexplored pages as bugged");
            error!("executing query {}", last_query);
            connection.query_drop(last_query).unwrap_or_else(|e| {
                error!("couldn't mark all unexplored pages as bugged");
                error!("{}", e);
            });

            let error_captures = error_regex.captures(&error);
            match error_captures {
                Some(capture) => {
                    let error_code = capture.get(1).unwrap().as_str().parse::<usize>().unwrap();
                    let count = error_count
                        .entry(error_code)
                        .and_modify(|counter| *counter += 1)
                        .or_insert(1);
                    if *count > MAX_SAME_ERROR {
                        error!("Too many same errors, stopping the program");
                        break;
                    } else {
                        info!("program restarting in 10 seconds ...");
                        info!("Press CTRL + C to stop.");
                        time::sleep(Duration::from_secs(10)).await;
                        continue;
                    }
                }
                None => {
                    error!("Couldn't find error code in error message");
                    error!("Stopping the program");
                }
            }
        }

        info!("WIKICRAWL FINISHED");
        return ();
    }
}

async fn wikicrawl(
    last_query: &mut String,
    exploring_pages: &mut Vec<Page>,
    total_info: &mut TotalInfo,
    connection: &mut PooledConn,
    sigint_cancel: &Arc<Mutex<bool>>,
    max_exploring_pages: usize,
    max_new_pages: usize,
) -> Result<(), Box<dyn Error>> {
    info!("");
    info!(
        "explored {} pages (with {} bugged)",
        total_info.explored, total_info.bugged
    );
    info!("found {} pages", total_info.pages);
    info!("listed {} links", total_info.links);
    info!("");

    while {
        let temp = sigint_cancel.lock().unwrap();
        !*temp
    } {
        // get unexplored pages
        last_query.clear();
        last_query.push_str(&format!(
            "SELECT id, title FROM Pages WHERE explored = false AND bugged = false ORDER BY id ASC LIMIT {};",
            max_exploring_pages
        ));
        info!("getting unexplored pages");
        exploring_pages.clear();
        exploring_pages.extend(
            connection
                .query_map(&last_query, |(id, title)| Page { id, title })?
                .into_iter(),
        );
        let unexplored_length = exploring_pages.len();
        if unexplored_length < 1 {
            error!("No unexplored pages found");
            return Err(Box::from("No unexplored pages found"));
        }
        info!(
            "Exploring pages: [{}]",
            exploring_pages
                .iter()
                .map(|page| page.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        );

        // delete potential links from an old run
        info!("deleting potential links from an old run");
        last_query.clear();
        last_query.push_str(&format!(
            "DELETE FROM Links WHERE linker IN ({});",
            exploring_pages
                .iter()
                .map(|page| page.id.to_string())
                .collect::<Vec<String>>()
                .join(", "),
        ));
        connection.query_drop(&last_query)?;

        let mut results: Vec<(Page, Vec<(String, String)>)> = Vec::new();
        let mut bugged_pages: Vec<Page> = Vec::new();
        let mut children: Vec<JoinHandle<(Page, Option<Vec<(String, String)>>)>> = Vec::new();
        let now = Instant::now();
        let shared_explored_count = Arc::new(Mutex::new(0 as usize));

        info!("exploring pages");

        let exploring_runtime = RuntimeBuilder::new_multi_thread()
            .worker_threads(unexplored_length)
            .enable_all()
            .thread_name("wikicrawl exploring".to_string())
            .build()?;
        exploring_pages.clone().into_iter().for_each(|page| {
            let thread_explored_count = Arc::clone(&shared_explored_count);
            let child = exploring_runtime.spawn(async move {
                let explore_result = explore(&page).await;
                let count = {
                    let mut tmp = thread_explored_count.lock().unwrap();
                    (*tmp).add_assign(1);
                    *tmp
                };
                print!(
                    "explored {}/{} pages ({}%)      \r",
                    count,
                    unexplored_length,
                    100 * count / unexplored_length
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

        print!("explored 0/{} pages (0%)      \r", unexplored_length);
        std::io::stdout().flush()?;
        for child in children.into_iter() {
            let (page, links) = child.await?;
            match links {
                Some(links) => results.push((page, links)),
                None => bugged_pages.push(page),
            }
        }
        exploring_runtime.shutdown_background();

        info!(
            "explored {} pages in {} ms",
            unexplored_length,
            now.elapsed().as_millis()
        );

        // mark as bugged if there are
        if !bugged_pages.is_empty() {
            last_query.clear();
            last_query.push_str(&format!(
                "UPDATE Pages SET bugged = TRUE WHERE id IN ({});",
                bugged_pages
                    .iter()
                    .map(|page| page.id.to_string())
                    .collect::<Vec<String>>()
                    .join(","),
            ));
            info!("marking bugged pages");
            connection.query_drop(&last_query)?;
            info!("marked {} bugged pages", bugged_pages.len());
            total_info.bugged += bugged_pages.len();
        }

        let found_links = results
            .iter()
            .map(|(_, links)| links.clone().into_iter().map(|(link, _display)| link))
            .flatten()
            .collect::<HashSet<String>>();
        info!("found {} links", found_links.len());

        if found_links.len() > 0 {
            let now = Instant::now();
            last_query.clear();
            last_query.push_str(&format!(
			"SELECT Alias.alias, Pages.id, Pages.title FROM Pages JOIN Alias ON Pages.id = Alias.id WHERE alias IN ({});", 
			found_links
				.iter()
				.map(|link| format!("\"{}\"", format_link_for_mysql(link)))
				.collect::<Vec<String>>()
				.join(", ")));
            let mut old_pages = connection
                .query_map(
                    &last_query,
                    |(alias, id, title): (String, usize, String)| (alias, Page { id, title }),
                )?
                .into_iter()
                .collect::<HashMap<String, Page>>();

            info!(
                "found {} old pages ({}ms)",
                old_pages.len(),
                now.elapsed().as_millis()
            );

            let new_links = found_links
                .into_iter()
                .filter(|link| !old_pages.contains_key(link))
                .collect::<Vec<String>>();
            let shared_count = Arc::new(Mutex::new(new_links.len()));
            let shared_links = Arc::new(Mutex::new(new_links.into_iter()));
            let shared_now = Arc::new(Mutex::new(Instant::now()));

            let new_pages_runtime = RuntimeBuilder::new_multi_thread()
                .worker_threads(max_new_pages.max({
                    let count = shared_count.lock().unwrap();
                    *count
                }))
                .enable_all()
                .thread_name("wikicrawl new_pages".to_string())
                .build()?;

            let new_pages_children = (0..max_new_pages).into_iter().map(|_| {
                let mut thread_pages: Vec<(String, Page)> = Vec::new();
                let thread_links = Arc::clone(&shared_links);
                let thread_count = Arc::clone(&shared_count);
                let thread_now = Arc::clone(&shared_now);
                new_pages_runtime.spawn(async move {
                    while let Some(link) = {
                        let mut links = thread_links.lock().unwrap();
                        (*links).next()
                    } {
                        let page = extract_link_info_api(&link).await;
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

            new_pages_runtime.shutdown_background();

            info!(
                "found {} pages ({}ms)               ",
                found_pages.len(),
                shared_now.lock().unwrap().elapsed().as_millis()
            );

            // split found_pages into new_pages and found_again_pages using the connection
            let found_again_pages_ids = if found_pages.is_empty() {
                Vec::new()
            } else {
                last_query.clear();
                last_query.push_str(&format!(
                    "SELECT id FROM Pages WHERE id IN ({});",
                    found_pages
                        .iter()
                        .map(|(_, page)| page.id.to_string())
                        .collect::<Vec<String>>()
                        .join(","),
                ));
                connection.query_map(&last_query, |id: usize| id)?
            };

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

            info!("found {} new pages", unique_new_pages.len(),);
            info!("found again {} old pages", old_pages.len());

            // insert new pages
            if added_pages > 0 {
                total_info.pages += added_pages;
                last_query.clear();
                last_query.push_str(&format!(
                    "INSERT INTO Pages (id, title) VALUES {};",
                    unique_new_pages
                        .into_iter()
                        .map(|page| {
                            format!("({}, \"{}\")", page.id, format_link_for_mysql(&page.title))
                        })
                        .collect::<Vec<String>>()
                        .join(","),
                ));
                info!("inserting new pages");
                connection.query_drop(&last_query)?;
                info!("inserted {} new pages", added_pages);
            }

            // insert aliases of new Pages
            if new_pages.len() > 0 {
                last_query.clear();
                last_query.push_str(&format!(
                    "INSERT INTO Alias (alias, id) VALUES {};",
                    new_pages
                        .iter()
                        .map(|(alias, page)| {
                            format!("(\"{}\",{})", format_link_for_mysql(alias), page.id)
                        })
                        .collect::<Vec<String>>()
                        .join(","),
                ));
                info!("inserting aliases of found pages");
                connection.query_drop(&last_query)?;
                info!("inserted {} aliases", new_pages.len());
            }

            // transform the results array into an array of relations between pages
            info!("generating relations ");
            let relations_found = results
                .iter()
                .map(|(page, links)| {
                    links
                        .iter()
                        .filter_map(|(link, display)| {
                            let linked = old_pages.get(link).or(new_pages.get(link));
                            linked.map(|link| Link {
                                linker: page.id,
                                linked: link.id,
                                display: display,
                            })
                        })
                        .collect::<HashSet<Link>>()
                })
                .flatten()
                .collect::<HashSet<Link>>();
            info!("generated {} relations", relations_found.len());

            // insert the new relations
            last_query.clear();
            last_query.push_str(&format!(
                "INSERT INTO Links (linker, linked, display) VALUES {};",
                relations_found
                    .iter()
                    .map(|link| format!(
                        "({},{},\"{}\")",
                        link.linker,
                        link.linked,
                        format_link_for_mysql(&link.display)
                    ))
                    .collect::<Vec<String>>()
                    .join(", "),
            ));
            info!("inserting the relations ");
            connection.query_drop(&last_query)?;
            info!("inserted {} relations", relations_found.len());
            total_info.links += relations_found.len();
        }

        // mark as explored
        last_query.clear();
        last_query.push_str(&format!(
            "UPDATE Pages SET explored = TRUE WHERE id IN ({});",
            exploring_pages
                .iter()
                .map(|page| page.id.to_string())
                .collect::<Vec<String>>()
                .join(", "),
        ));
        info!("marking pages as explored ");
        connection.query_drop(&last_query)?;
        info!("explored {} pages", unexplored_length);
        total_info.explored += unexplored_length;

        info!("");
        info!(
            "explored {} pages (with {} bugged)",
            total_info.explored, total_info.bugged
        );
        info!("found {} pages", total_info.pages);
        info!("listed {} links", total_info.links);
        info!("");
    }

    return Ok(());
}

async fn explore(page: &Page) -> Result<Vec<(String, String)>, Box<dyn Error>> {
    let request = format!("https://fr.m.wikipedia.org/?curid={}", page.id);

    let mut retry_cooldown = RETRY_COOLDOWN.clone();
    let delta_t = Duration::from_secs(1);
    loop {
        retry_cooldown.add_assign(delta_t);
        let body = CLIENT
            .get(request.clone())
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        if body.contains("<title>Wikimedia Error</title>") {
            warn!("exploring {} throwed wikimedia error", page);
            time::sleep(retry_cooldown).await;
            continue;
        }

        let found_links = EXPLORE_REGEX
            .captures_iter(body.as_str())
            .map(|captures| {
                let link = decode(captures.get(1).unwrap().as_str())
                    .unwrap()
                    .into_owned()
                    .to_ascii_lowercase();
                let display = captures.get(2).unwrap().as_str().to_string();
                (link, display)
            })
            .collect::<HashSet<(String, String)>>();

        let filtered_links = found_links
            .into_iter()
            .filter(|(link, _display)| {
                !WIKIPEDIA_NAMESPACES
                    .iter()
                    .any(|namespace| link.starts_with(namespace))
            })
            .collect::<Vec<(String, String)>>();

        if filtered_links.is_empty() {
            warn!(
                "No links found in Page {{ id: {}, title: \"{}\" }}",
                page.id, page.title
            );
        }

        return Ok(filtered_links);
    }
}

fn setup_logs() -> Result<(), Box<dyn Error>> {
    std::fs::DirBuilder::new().recursive(true).create("logs")?;
    let log_pattern = "{d(%Y-%m-%d_%H:%M:%S)}-[{l}]: {m}{n}";

    let mut log_name = Local::now().format("%Y-%m-%d").to_string();
    let this_day_logs = std::fs::read_dir("logs")?
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
    log_name.push_str(&format!("_{}", this_day_logs));

    let day_file = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(log_pattern)))
        .build(format!("logs/{}.log", log_name))
        .unwrap();

    let stdout = ConsoleAppender::builder()
        .target(Target::Stdout)
        .encoder(Box::new(PatternEncoder::new(log_pattern)))
        .build();

    std::fs::write("logs/latest.log", "").unwrap_or_default();
    let latest_file = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(log_pattern)))
        .build("logs/latest.log")
        .unwrap();

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("day_file", Box::new(day_file)))
        .appender(Appender::builder().build("latest_file", Box::new(latest_file)))
        .build(
            Root::builder()
                .appender("stdout")
                .appender("day_file")
                .appender("latest_file")
                .build(LevelFilter::Info),
        )
        .unwrap();

    log4rs::init_config(config).unwrap();

    Ok(())
}
