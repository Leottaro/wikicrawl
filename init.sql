DROP TABLE IF EXISTS Links;
DROP TABLE IF EXISTS Alias;
DROP TABLE IF EXISTS Pages;

CREATE TABLE IF NOT EXISTS Pages (
  id INT UNSIGNED UNIQUE NOT NULL,
  title VARCHAR(255) COLLATE utf8mb4_bin UNIQUE NOT NULL,
  explored BOOLEAN DEFAULT false,
  bugged BOOLEAN DEFAULT false,
  KEY id_index (id),
  FULLTEXT KEY title_fulltext (title),
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS Alias (
  alias VARCHAR(255) COLLATE utf8mb4_bin UNIQUE NOT NULL,
  id INT UNSIGNED NOT NULL,
  KEY id_index (id),
  FULLTEXT KEY alias_fulltext (alias),
  FOREIGN KEY id_foreign (id) REFERENCES Pages(id),
  PRIMARY KEY (alias, id)
);

CREATE TABLE IF NOT EXISTS Links (
  linker INT UNSIGNED NOT NULL,
  linked INT UNSIGNED NOT NULL,
  display VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
  KEY linker_index (linker),
  KEY linked_index (linked),
  FULLTEXT KEY display_fulltext (display),
  FOREIGN KEY linker_foreign (linker) REFERENCES Pages(id),
  FOREIGN KEY linked_foreign (linked) REFERENCES Pages(id),
  PRIMARY KEY (linker, linked, display)
);

INSERT INTO Pages (id, title) VALUES (1095, "France");
