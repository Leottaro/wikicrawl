use lib::*;

use mysql::{prelude::Queryable, PooledConn};
use std::{
    collections::HashMap,
    io::{stdin, stdout, Write},
};

pub async fn setup_wikipath(connection: &mut PooledConn) {
    let start_page = get_page(
        connection,
        "\nFrom which page do you want to start ? (enter page url or title) \n-> ",
    )
    .await;
    println!("start Page {}", start_page);

    let end_page = get_page(
        connection,
        "\nTo which page do you want to go ? (enter page url or title) \n-> ",
    )
    .await;
    println!("end Page {}", end_page);

    let mut last_query = String::new();
    let result = wikipath(&mut last_query, connection, start_page, end_page);
    if result.is_err() {
        println!("WIKICRAWL CRASHED WITH LAST QUERY BEING \n{}", last_query);
        println!("Error: {:?}", result.unwrap_err());
        return;
    } else {
        let pages = result.unwrap();
        println!(
            "The smallest path is: \n{{\n{}\n}}",
            pages
                .into_iter()
                .map(|page| format!("->  Page: {}", page))
                .collect::<Vec<String>>()
                .join("\n")
        );
    }
}

async fn get_page(connection: &mut PooledConn, request_message: &str) -> Page {
    let mut user_input = String::new();
    loop {
        print!("{}", request_message);
        stdout().flush().unwrap();
        user_input.clear();
        stdin().read_line(&mut user_input).unwrap();
        let page_title = {
            if user_input.starts_with("http") {
                let temp = user_input.split("wiki/").last().unwrap();
                if temp.starts_with("Spécial:Recherche/") {
                    temp.split_at(19).1
                } else {
                    temp
                }
            } else {
                user_input.as_str()
            }
        }
        .to_ascii_lowercase();

        let page = connection
			.query_map(format!(
				"SELECT Pages.id, Pages.title FROM Pages JOIN Alias ON Pages.id = Alias.id WHERE LOWER(title) = \"{}\" OR alias = \"{}\";",
				format_link_for_mysql(&page_title),
				format_link_for_mysql(&page_title)
			), |(id, title): (usize, String)| Page { id, title }).unwrap_or(Vec::new());

        let page = if page.is_empty() {
            extract_link_info_api(&page_title).await
        } else {
            page.first().unwrap().to_owned()
        };

        if connection
            .query_first::<(usize, String), _>(format!(
                "SELECT * FROM Pages WHERE id = {} AND explored = TRUE;",
                page.id
            ))
            .unwrap()
            .is_some()
        {
            return page;
        }
        println!("That page isn't explored yet");
    }
}

fn wikipath(
    last_query: &mut String,
    connection: &mut PooledConn,
    start_page: Page,
    end_page: Page,
) -> Result<Vec<Page>, mysql::Error> {
    // exploring the database
    // we only collect the first time where a page is linked to another page
    // because we are looking for the shortest path, since we are exploring the database depth by depth,
    // we can be sure that the first time we find a page, it's one of the shortest path to it
    let mut is_linked_first: HashMap<usize, usize> = HashMap::new();
    let mut exploring_pages_id: Vec<usize> = vec![start_page.id];

    'truc: for depth in 0.. {
        println!("exploring depth {}", depth);
        let mut next_exploring_pages_id: Vec<usize> = Vec::new();

        let mut i = 0;
        for exploring_chunk in exploring_pages_id.chunks(8192) {
            i += exploring_chunk.len();
            last_query.clear();
            last_query.push_str(&format!(
                "SELECT * FROM Links WHERE linker IN ({});",
                exploring_chunk
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<String>>()
                    .join(",")
            ));

            let pages = connection.query_map(&last_query, |(linker, linked): (usize, usize)| {
                (linker, linked)
            })?;

            pages.clone().into_iter().for_each(|(linker, linked)| {
                if !is_linked_first.contains_key(&linked) {
                    next_exploring_pages_id.push(linked);
                    is_linked_first.insert(linked, linker);
                }
            });

            if pages
                .iter()
                .find(|(_, linked)| end_page.id.eq(linked))
                .is_some()
            {
                println!("found end page");
                break 'truc;
            }
            print!(
                "\rexplored {}/{} ({}%)",
                i,
                exploring_pages_id.len(),
                i * 100 / exploring_pages_id.len()
            );
            stdout().flush().unwrap();
        }
        println!();

        if next_exploring_pages_id.is_empty() {
            return Err(mysql::Error::MySqlError(mysql::MySqlError {
                code: 0,
                state: "".to_string(),
                message: "No path found".to_string(),
            }));
        }

        exploring_pages_id.clear();
        exploring_pages_id.extend(next_exploring_pages_id.into_iter());
    }

    // backtrack the smallest path
    print!("backtracking the smallest path \n{}", end_page.id);
    let mut path: Vec<usize> = vec![end_page.id];
    while let Some(&last_page) = path.last() {
        let next_page = is_linked_first.get(&last_page).unwrap();
        path.push(next_page.clone());
        print!(" -> {}", next_page);
        if next_page.eq(&start_page.id) {
            break;
        }
    }
    println!();

    // convert the path from id to title
    last_query.clear();
    last_query.push_str(&format!(
        "SELECT id,title from Pages where id IN ({});",
        path.iter()
            .map(|id| id.to_string())
            .collect::<Vec<String>>()
            .join(",")
    ));

    println!("converting the path from id to title");
    let id_to_title = connection
        .query_map(&last_query, |(id, title): (usize, String)| (id, title))?
        .into_iter()
        .collect::<HashMap<usize, String>>();

    let final_path = path
        .into_iter()
        .map(|id| Page {
            id,
            title: id_to_title.get(&id).unwrap().clone(),
        })
        .rev()
        .collect::<Vec<Page>>();

    Ok(final_path)
}
