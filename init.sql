DROP TABLE IF EXISTS Links;
DROP TABLE IF EXISTS Alias;
DROP TABLE IF EXISTS Pages;

CREATE TABLE IF NOT EXISTS Pages (
  id INT UNSIGNED UNIQUE NOT NULL,
  title VARCHAR(255) COLLATE utf8mb4_bin UNIQUE NOT NULL,
  explored BOOLEAN DEFAULT false,
  bugged BOOLEAN DEFAULT false,
  PRIMARY KEY (id),
  FULLTEXT(title)
);

CREATE TABLE IF NOT EXISTS Alias (
  alias VARCHAR(255) COLLATE utf8mb4_bin UNIQUE NOT NULL,
  id INT UNSIGNED NOT NULL,
  PRIMARY KEY (alias, id),
  FOREIGN KEY (id) REFERENCES Pages(id),
  FULLTEXT(alias)
);

CREATE TABLE IF NOT EXISTS Links (
  linker INT UNSIGNED NOT NULL,
  linked INT UNSIGNED NOT NULL,
  PRIMARY KEY (linker, linked),
  FOREIGN KEY (linker) REFERENCES Pages(id),
  FOREIGN KEY (linked) REFERENCES Pages(id)
);

INSERT INTO Pages (id, title) VALUES (1095, "France");