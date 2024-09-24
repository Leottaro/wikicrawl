use lib::*;

use chrono::Local;
use env_logger::Builder as EnvBuilder;
use log::LevelFilter;
use mysql::{prelude::*, PooledConn};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::File;
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

pub async fn setup_wikicrawl(
    connection: &mut PooledConn,
    max_exploring_pages: usize,
    max_new_pages: usize,
) -> () {
    println!("setting up logs");
    setup_logs().unwrap();
    println_and_log("Starting wikicrawl");

    loop {
        let sigint_cancel = Arc::new(Mutex::new(false));
        let mut last_query: String = String::new();
        let result = wikicrawl(
            &mut last_query,
            &sigint_cancel,
            connection,
            max_exploring_pages,
            max_new_pages,
        )
        .await;
        *sigint_cancel.lock().unwrap() = true;
        if result.is_err() {
            if !last_query.is_empty() {
                error_and_log("WIKICRAWL CRASHED WITH LAST QUERY BEING");
                error_and_log(&format!("{}", last_query));
            } else {
                error_and_log("WIKICRAWL CRASHED");
            }
            error_and_log(&format!("{}", result.unwrap_err()));
            println_and_log("program restarting in 10 seconds ...");
            println_and_log("Press CTRL + C to stop.");
            time::sleep(Duration::from_secs(10)).await;
        } else {
            println_and_log("WIKICRAWL FINISHED");
            return ();
        }
    }
}

async fn wikicrawl(
    last_query: &mut String,
    sigint_cancel: &Arc<Mutex<bool>>,
    connection: &mut PooledConn,
    max_exploring_pages: usize,
    max_new_pages: usize,
) -> Result<(), Box<dyn Error>> {
    println_and_log("querying total explored pages");
    let mut total_explored: usize = connection
        .query_first("SELECT COUNT(*) FROM Pages WHERE explored = TRUE;")
        .unwrap_or(Some(0))
        .unwrap_or(0);
    println_and_log("querying total bugged pages");
    let mut total_bugged: usize = connection
        .query_first("SELECT COUNT(*) FROM Pages WHERE bugged = TRUE;")
        .unwrap_or(Some(0))
        .unwrap_or(0);
    println_and_log("querying total pages");
    let mut total_pages: usize = connection
        .query_first("SELECT COUNT(*) FROM Pages;")
        .unwrap_or(Some(0))
        .unwrap_or(0);
    println_and_log("querying total links");
    let mut total_links: usize = connection
        .query_first("SELECT COUNT(*) FROM Links;")
        .unwrap_or(Some(0))
        .unwrap_or(0);

    println_and_log(&format!(
        "explored {} pages (with {} bugged) \nfound {} pages \nlisted {} links\n",
        total_explored, total_bugged, total_pages, total_links
    ));

    {
        println_and_log("creating SIGINT thread");
        let sigint_cancel_clone = Arc::clone(sigint_cancel);
        ctrlc::set_handler(move || sigint_detect(&sigint_cancel_clone))?;
    }

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
        println_and_log("getting unexplored pages");
        let unexplored_pages =
            connection.query_map(&last_query, |(id, title)| Page { id, title })?;
        let unexplored_length = unexplored_pages.len();
        if unexplored_length < 1 {
            return Err(Box::from("No unexplored pages found"));
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
        last_query.clear();
        last_query.push_str(&format!(
            "DELETE FROM Links WHERE linker IN ({});",
            unexplored_pages
                .iter()
                .map(|page| page.id.to_string())
                .collect::<Vec<String>>()
                .join(", "),
        ));
        connection.query_drop(&last_query)?;

        let mut results: Vec<(Page, Vec<String>)> = Vec::new();
        let mut bugged_pages: Vec<Page> = Vec::new();
        let mut children: Vec<JoinHandle<(Page, Option<Vec<String>>)>> = Vec::new();
        let now = Instant::now();
        let shared_explored_count = Arc::new(Mutex::new(0 as usize));

        println_and_log(&format!("exploring pages"));

        let exploring_runtime = RuntimeBuilder::new_multi_thread()
            .worker_threads(unexplored_length)
            .enable_all()
            .thread_name("wikicrawl exploring".to_string())
            .build()?;

        unexplored_pages.into_iter().for_each(|page| {
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

        println_and_log(&format!(
            "explored {} pages in {} ms",
            unexplored_length,
            now.elapsed().as_millis()
        ));

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
            println_and_log("marking bugged pages");
            connection.query_drop(&last_query)?;
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

            println_and_log(&format!(
                "found {} pages ({}ms)               ",
                found_pages.len(),
                shared_now.lock().unwrap().elapsed().as_millis()
            ));

            // split found_pages into new_pages and found_again_pages using the connection
            last_query.clear();
            last_query.push_str(&format!(
                "SELECT id FROM Pages WHERE id IN ({});",
                found_pages
                    .iter()
                    .map(|(_, page)| page.id.to_string())
                    .collect::<Vec<String>>()
                    .join(","),
            ));
            let found_again_pages_ids = connection.query_map(&last_query, |id: usize| id)?;

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

            println_and_log(&format!("found {} new pages", unique_new_pages.len(),));
            println_and_log(&format!("found again {} old pages", old_pages.len()));

            // insert new pages
            if added_pages > 0 {
                total_pages += added_pages;
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
                println_and_log("inserting new pages");
                connection.query_drop(&last_query)?;
                println_and_log(&format!("inserted {} new pages", added_pages));
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
                println_and_log("inserting aliases of found pages");
                connection.query_drop(&last_query)?;
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
            last_query.clear();
            last_query.push_str(&format!(
                "INSERT INTO Links (linker, linked) VALUES {};",
                relations_found
                    .iter()
                    .map(|(linker, linked)| format!("({},{})", linker.id, linked.id))
                    .collect::<Vec<String>>()
                    .join(", "),
            ));
            println_and_log("inserting the relations ");
            connection.query_drop(&last_query)?;
            println_and_log(&format!("inserted {} relations", relations_found.len()));
            total_links += relations_found.len();
        }

        // mark as explored
        last_query.clear();
        last_query.push_str(&format!(
            "UPDATE Pages SET explored = TRUE WHERE id IN ({});",
            results
                .iter()
                .map(|(page, _)| page.id.to_string())
                .collect::<Vec<String>>()
                .join(", "),
        ));
        println_and_log("marking pages as explored ");
        connection.query_drop(&last_query)?;
        println_and_log(&format!("explored {} pages", unexplored_length));
        total_explored += unexplored_length;

        println_and_log(&format!(
            "explored {} pages (with {} bugged) \nfound {} pages \nlisted {} links\n",
            total_explored, total_bugged, total_pages, total_links
        ));
    }

    return Ok(());
}

async fn explore(page: &Page) -> Result<Vec<String>, Box<dyn Error>> {
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
            warn_and_log(&format!("exploring {} throwed wikimedia error", page));
            time::sleep(retry_cooldown).await;
            continue;
        }

        let found_links = EXPLORE_REGEX
            .captures_iter(body.as_str())
            .map(|captures| {
                decode(captures.get(1).unwrap().as_str())
                    .unwrap()
                    .into_owned()
                    .to_ascii_lowercase()
            })
            .collect::<HashSet<String>>();

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

fn setup_logs() -> Result<(), Box<dyn Error>> {
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
        log_name.push_str(&format!("_{}", this_day_logs + 1));
    }

    let target =
        Box::new(File::create(format!("logs/{}.log", log_name)).expect("Can't create file"));

    EnvBuilder::new()
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

fn sigint_detect(cancel: &Arc<Mutex<bool>>) -> () {
    let mut cancel = cancel.lock().unwrap();
    if !*cancel {
        println_and_log("SIGINT received, waiting for the program to stop");
        *cancel = true;
    } else {
        println_and_log("SIGINT received, forcing the program to stop");
        std::process::exit(0);
    }
}
